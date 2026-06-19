// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use quickwit_common::uri::Uri;
use quickwit_config::{S3StorageConfig, StorageBackend};
use tokio::sync::OnceCell;

use super::s3_compatible_storage::create_s3_client;
use crate::{
    DebouncedStorage, S3CompatibleObjectStorage, Storage, StorageFactory, StorageResolverError,
};

/// Extracts the named-backend key out of an `s3+<name>://...` URI, if any.
/// Returns `None` for plain `s3://...`.
fn parse_named_key(uri: &Uri) -> Option<&str> {
    let scheme_end = uri.as_str().find("://")?;
    let scheme = &uri.as_str()[..scheme_end];
    scheme.strip_prefix("s3+")
}

/// S3 compatible object storage resolver.
pub struct S3CompatibleObjectStorageFactory {
    storage_config: S3StorageConfig,
    // we cache the S3Client so we don't rebuild one every time we build a new Storage (for
    // every search query).
    // We don't build it in advance because we don't know if this factory is one that will
    // end up being used, or if something like azure, gcs, or even local files, will be used
    // instead.
    s3_client: OnceCell<S3Client>,
    // One cached S3Client per named backend, each behind its own `OnceCell` so
    // backends initialize independently. The `Mutex` is only ever held
    // synchronously to look up / insert the per-name cell — never across the
    // client-building await.
    named_s3_clients: Mutex<HashMap<String, Arc<OnceCell<S3Client>>>>,
}

impl S3CompatibleObjectStorageFactory {
    /// Creates a new S3-compatible storage factory.
    pub fn new(storage_config: S3StorageConfig) -> Self {
        Self {
            storage_config,
            s3_client: OnceCell::new(),
            named_s3_clients: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl StorageFactory for S3CompatibleObjectStorageFactory {
    fn backend(&self) -> StorageBackend {
        StorageBackend::S3
    }

    async fn resolve(&self, uri: &Uri) -> Result<Arc<dyn Storage>, StorageResolverError> {
        if let Some(name) = parse_named_key(uri) {
            let named_config = self
                .storage_config
                .named
                .get(name)
                .ok_or_else(|| {
                    StorageResolverError::InvalidUri(format!(
                        "no `storage.s3.named.{name}` entry configured for URI `{uri}`"
                    ))
                })?
                .as_s3_config();
            let client_cell = {
                let mut clients = self
                    .named_s3_clients
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                Arc::clone(clients.entry(name.to_string()).or_default())
            };
            let client = client_cell
                .get_or_init(|| create_s3_client(&named_config))
                .await
                .clone();
            let storage =
                S3CompatibleObjectStorage::from_uri_and_client(&named_config, uri, client).await?;
            return Ok(Arc::new(DebouncedStorage::new(storage)));
        }
        let s3_client = self
            .s3_client
            .get_or_init(|| create_s3_client(&self.storage_config))
            .await
            .clone();
        let storage =
            S3CompatibleObjectStorage::from_uri_and_client(&self.storage_config, uri, s3_client)
                .await?;
        Ok(Arc::new(DebouncedStorage::new(storage)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_named_key() {
        // Plain s3:// URIs route through the primary backend.
        assert_eq!(parse_named_key(&Uri::for_test("s3://bucket/key")), None);
        // `s3+<name>` URIs return the named-backend key.
        assert_eq!(
            parse_named_key(&Uri::for_test("s3+alt://bucket/key")),
            Some("alt")
        );
        assert_eq!(
            parse_named_key(&Uri::for_test("s3+with-dash://bucket/key")),
            Some("with-dash")
        );
    }

    #[tokio::test]
    #[cfg_attr(not(feature = "ci-test"), ignore)]
    async fn test_named_backends_cache_independently() {
        use std::collections::BTreeMap;

        use quickwit_config::NamedS3StorageConfig;

        let mut named = BTreeMap::new();
        for backend in ["alt", "other"] {
            named.insert(
                backend.to_string(),
                NamedS3StorageConfig {
                    endpoint: Some("http://localhost:4566".to_string()),
                    region: Some("us-east-1".to_string()),
                    force_path_style_access: true,
                    ..Default::default()
                },
            );
        }
        let storage_config = S3StorageConfig {
            named,
            ..Default::default()
        };
        let factory = S3CompatibleObjectStorageFactory::new(storage_config);

        // Distinct named backends each resolve into their own cached cell.
        factory
            .resolve(&Uri::for_test("s3+alt://bucket/a"))
            .await
            .unwrap();
        factory
            .resolve(&Uri::for_test("s3+other://bucket/b"))
            .await
            .unwrap();
        // Re-resolving a backend reuses the cached, initialized cell.
        factory
            .resolve(&Uri::for_test("s3+alt://bucket/c"))
            .await
            .unwrap();

        let clients = factory
            .named_s3_clients
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(clients.len(), 2);
        assert!(clients.get("alt").unwrap().initialized());
        assert!(clients.get("other").unwrap().initialized());
    }
}
