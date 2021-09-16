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

use crate::types::FILES_KEY;
use crate::types::{
    extract_chunk_number, extract_complete_file_path, extract_file_path, hash_path, FileMetadata,
    ServerConfig,
};

use crate::utils::{
    create_dir_if_not_exists, read_file_to_string, read_file_to_vec, write_chunk_file,
    write_metadata_file,
};

use async_std::sync::Arc;
use async_std::task::JoinHandle;
use futures::prelude::*;
use futures::select;
use futures::StreamExt;
use std::convert::TryFrom;
use std::path::Path;

use zenoh::{Change, ChangeKind, GetRequest, Value, ZError, ZErrorKind, ZResult, Zenoh};
use zenoh::{Path as ZPath, PathExpr, Selector};
use zenoh_util::{zerror, zerror2};

pub fn hash(filename: &Path) -> String {
    checksums::hash_file(filename, checksums::Algorithm::MD5)
}

#[derive(Clone)]
pub struct Server {
    pub z: Arc<Zenoh>,
    pub config: ServerConfig,
}

impl Server {
    pub fn new(z: Arc<Zenoh>, config: ServerConfig) -> Self {
        Self { z, config }
    }

    pub fn serve(&self) -> ZResult<JoinHandle<ZResult<()>>> {
        let cloned_self = self.clone();
        let handle = async_std::task::spawn(async move { cloned_self.run().await });
        Ok(handle)
    }

    pub async fn run(&self) -> ZResult<()> {
        let ws = self.z.workspace(None).await?;

        let resource_space = Selector::try_from(self.config.resource_space.clone())?;

        let _resource_prefix = format!(
            "{}/{}",
            self.config
                .resource_space
                .clone()
                .split("/**")
                .collect::<Vec<&str>>()[0],
            FILES_KEY
        );

        let mut change_stream = ws.subscribe(&resource_space).await?;
        let mut get_stream = ws
            .register_eval(&PathExpr::try_from(self.config.resource_space.clone())?)
            .await?;

        loop {
            select! {
                sample = change_stream.next().fuse() => {

                    match self.process_sample(sample).await {
                        Ok(_) => (),
                        Err(e) => log::error!("Process file storage failed: {:?}",e ),
                    }

                }
                query = get_stream.next().fuse() => {
                    match self.process_query(query).await {
                        Ok(_) => (),
                        Err(e) => log::error!("Process file retrieve failed: {:?}",e ),
                    }
                }
            }
        }
    }

    async fn process_query(&self, query: Option<GetRequest>) -> ZResult<()> {
        let resource_prefix = format!(
            "{}/{}",
            self.config
                .resource_space
                .clone()
                .split("/**")
                .collect::<Vec<&str>>()[0],
            FILES_KEY
        );

        let query = match query {
            Some(s) => Ok(s),
            None => zerror!(ZErrorKind::Other {
                descr: "Eval received nothing".to_string()
            }),
        }?;

        match query.selector.path_expr.is_a_path() {
            false => zerror!(ZErrorKind::Other {
                descr: format!("Malformend query {:?}", query.selector)
            }),
            _ => Ok(()),
        }?;
        let query_path = query.selector.path_expr.as_str();

        log::debug!("Received query {:?}", query_path);
        let complete_path = extract_complete_file_path(&resource_prefix, query_path)?;

        let resp: Value = match extract_chunk_number(&complete_path) {
            Err(_) => {
                log::debug!("Getting metadata");
                let hashed_path = hash_path(&complete_path);
                let metadata_path = self.config.chunks_dir.join(&hashed_path).join("metadata");
                log::debug!(
                    "Getting metadata for {:?} - reading from {:?}",
                    complete_path,
                    metadata_path
                );
                let metadata = read_file_to_string(&metadata_path).await?;
                Value::Json(metadata)
            }
            Ok(chunk_number) => {
                log::debug!("Getting chunk");
                let path = extract_file_path(&resource_prefix, query_path)?;
                let hashed_path = hash_path(&path);

                let chunk_path = self
                    .config
                    .chunks_dir
                    .join(&hashed_path)
                    .join(&format!("{}", chunk_number));
                log::debug!(
                    "Getting chunk {:?} for {:?} - reading from {:?}",
                    chunk_number,
                    path,
                    chunk_path
                );
                let data = read_file_to_vec(&chunk_path).await?;
                data.into()
            }
        };

        query.reply_async(ZPath::try_from(query_path)?, resp).await;
        Ok(())
    }

    async fn process_sample(&self, sample: Option<Change>) -> ZResult<()> {
        let resource_prefix = format!(
            "{}/{}",
            self.config
                .resource_space
                .clone()
                .split("/**")
                .collect::<Vec<&str>>()[0],
            FILES_KEY
        );

        let sample = match sample {
            Some(s) => Ok(s),
            None => zerror!(ZErrorKind::Other {
                descr: "Subscriber received nothing".to_string(),
            }),
        }?;
        log::debug!("Received data from {:?}", sample.path);
        match sample.kind {
            ChangeKind::Put | ChangeKind::Patch => {
                let value = sample.value.ok_or_else(|| {
                    zerror2!(ZErrorKind::Other {
                        descr: "Sample is missing value".to_string(),
                    })
                })?;

                match value {
                    Value::Raw(_, buf) => {
                        let data = buf.to_vec();
                        log::debug!("Received {:?} bytes", data.len());

                        let path = extract_file_path(&resource_prefix, sample.path.as_str())?;
                        let chunk_number = extract_chunk_number(sample.path.as_str())?;
                        let hashed_path = hash_path(&path);

                        let complete_path = self.config.chunks_dir.join(&hashed_path);

                        log::debug!(
                            "Received {:?} Chunk {:?} - Hashed {:?} - Going to be stored in {:?}",
                            path,
                            chunk_number,
                            hashed_path,
                            complete_path
                        );

                        create_dir_if_not_exists(&complete_path).await?;

                        let chunk_path = complete_path.join(&format!("{}", chunk_number));

                        log::debug!(
                            "Received {:?} Chunk {:?} - Hashed {:?} - Going to be stored in {:?}",
                            path,
                            chunk_number,
                            hashed_path,
                            chunk_path
                        );
                        Ok(write_chunk_file(&chunk_path, &data).await?)
                    }
                    Value::Json(value) => {
                        let metadata = FileMetadata::deserialize(&value)?;
                        let path = metadata.resource_name.clone();
                        let hashed_path = hash_path(&path);
                        let metadata_path =
                            self.config.chunks_dir.join(&hashed_path).join("metadata");

                        log::debug!(
                            "Received Metadata {:?} - Going to be stored in {:?}",
                            metadata,
                            metadata_path
                        );

                        Ok(write_metadata_file(&metadata_path, &value).await?)
                    }
                    _ => {
                        log::error!("Subscriber received data not correctly formatted");
                        Ok(())
                    }
                }
            }
            ChangeKind::Delete => {
                //We should delete the chunk in this case.
                log::trace!("We should delete the chunk");
                Ok(())
            }
        }
    }
}
