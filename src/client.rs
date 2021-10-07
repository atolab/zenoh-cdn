//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

use crate::{FILE_CHUNK_PATH, FILE_METADATA_PATH};

use crate::types::{FileMetadata, DEFAULT_CHUNK_SIZE, DEFAULT_ROOT};
use crate::utils::{create_destination_file, get_bytes_from_file, write_destination_file};
use async_std::fs;
use async_std::path::PathBuf;
use async_std::prelude::*;
use async_std::sync::Arc;
use std::path::Path;
use zenoh::query::Reply;
use zenoh::{prelude::*, Session};
use zenoh_util::{zerror, zerror2};

pub fn hash(filename: &Path) -> String {
    checksums::hash_file(filename, checksums::Algorithm::MD5)
}

#[derive(Clone)]
pub struct Client {
    pub z: Arc<Session>,
    pub root: String,
}

impl Client {
    pub fn new(z: Arc<Session>, root: Option<String>) -> Self {
        Self {
            z,
            root: root.unwrap_or_else(|| String::from(DEFAULT_ROOT)),
        }
    }

    /// Uploads a file to Zenoh-CDN.
    ///
    pub async fn upload(&self, file_path: &Path, resource_name: &str) -> ZResult<String> {
        let filename = match file_path.file_name() {
            Some(name) => Ok(name.to_str().unwrap().to_string()),
            None => Err(zerror2!(ZErrorKind::Other {
                descr: format!("The path is not a file path {:?}", file_path)
            })),
        }?;

        let checksum = hash(file_path);
        let file_metadata = fs::metadata(file_path).await.map_err(|e| {
            zerror2!(ZErrorKind::Other {
                descr: format!("Error when getting file {:?} information {}", file_path, e)
            })
        })?;

        let chunks = (file_metadata.len() as usize) / DEFAULT_CHUNK_SIZE + 1;

        let metadata = FileMetadata {
            filename,
            checksum,
            chunk_size: DEFAULT_CHUNK_SIZE,
            chunks,
            resource_name: resource_name.to_string(),
            size: file_metadata.len(),
        };

        for i in 0..chunks {
            let data = get_bytes_from_file(file_path, i, DEFAULT_CHUNK_SIZE).await?;
            let path = FILE_CHUNK_PATH!(self.root, resource_name, i);
            let value = Value::new(data.into()).encoding(Encoding::APP_OCTET_STREAM);
            self.z.put(&path, value).await?;
        }

        let path = FILE_METADATA_PATH!(self.root, resource_name);
        let data = metadata.serialize()?;
        let value = Value::new(data.as_bytes().into()).encoding(Encoding::APP_JSON);
        self.z.put(&path, value).await?;

        Ok(path)
    }

    pub async fn download(&self, resource_name: &str, destination: &Path) -> ZResult<PathBuf> {
        let selector = FILE_METADATA_PATH!(self.root, resource_name);
        let metadata = {
            let ds = self.z.get(&selector).await?;

            // Not sure this is needed...
            let mut data = ds.collect::<Vec<Reply>>().await;

            match data.len() {
                0 => zerror!(ZErrorKind::Other {
                    descr: format!("File not found {:?}", resource_name)
                }),
                1 => {
                    let reply = data.remove(0);
                    let sample = reply.data;
                    match sample.value.encoding.prefix {
                        5 => {
                            //Encoding::APP_JSON => {
                            let value =
                                String::from_utf8(sample.value.payload.to_vec()).map_err(|e| {
                                    zerror2!(ZErrorKind::Other {
                                        descr: format!("Malformed metadata {:?}", e)
                                    })
                                })?;
                            Ok(FileMetadata::deserialize(&value)?)
                        }
                        _ => zerror!(ZErrorKind::Other {
                            descr: format!(
                                "Metadata is not correctly formatted {:?} - {:?}",
                                resource_name, sample
                            )
                        }),
                    }
                }
                _ => zerror!(ZErrorKind::Other {
                    descr: format!(
                        "Got more than one response with this filename {:?}",
                        resource_name
                    )
                }),
            }
        }?;

        let destination_file = create_destination_file(destination, metadata.size).await?;

        for i in 0..metadata.chunks {
            let selector = FILE_CHUNK_PATH!(self.root, resource_name, i);
            let data: Vec<u8> = {
                let ds = self.z.get(&selector).await?;

                // Not sure this is needed...
                let mut data = ds.collect::<Vec<Reply>>().await;

                match data.len() {
                    0 => zerror!(ZErrorKind::Other {
                        descr: format!("File not found {:?}", resource_name)
                    }),
                    1 => {
                        let reply = data.remove(0);
                        let sample = reply.data;
                        match sample.value.encoding.prefix {
                            1 => Ok(sample.value.payload.to_vec()), //Encoding::APP_OCTET_STREAM => Ok(sample.value.payload.to_vec()),
                            _ => zerror!(ZErrorKind::Other {
                                descr: format!(
                                    "File data format is not correctly formatted {:?} - {:?}",
                                    resource_name, sample
                                )
                            }),
                        }
                    }
                    _ => zerror!(ZErrorKind::Other {
                        descr: format!(
                            "Got more than one response with this filename {:?}",
                            resource_name
                        )
                    }),
                }
            }?;
            write_destination_file(&destination_file, &data, i, metadata.chunk_size).await?;
        }

        Ok(destination.into())
    }
}
