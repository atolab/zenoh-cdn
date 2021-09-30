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
use std::path::Path;
use zenoh::queryable::EVAL;

use zenoh::queryable::Query;
use zenoh::{prelude::*, Session};
use zenoh_util::{zerror, zerror2};

pub fn hash(filename: &Path) -> String {
    checksums::hash_file(filename, checksums::Algorithm::MD5)
}

#[derive(Clone)]
pub struct Server {
    pub z: Arc<Session>,
    pub config: ServerConfig,
}

impl Server {
    pub fn new(z: Arc<Session>, config: ServerConfig) -> Self {
        Self { z, config }
    }

    pub fn serve(&self) -> ZResult<JoinHandle<ZResult<()>>> {
        let cloned_self = self.clone();
        let handle = async_std::task::spawn(async move { cloned_self.run().await });
        Ok(handle)
    }

    pub async fn run(&self) -> ZResult<()> {
        let _resource_prefix = format!(
            "{}/{}",
            self.config
                .resource_space
                .clone()
                .split("/**")
                .collect::<Vec<&str>>()[0],
            FILES_KEY
        );

        let mut subscriber = self.z.subscribe(&self.config.resource_space).await?;
        let mut queryable = self
            .z
            .register_queryable(&self.config.resource_space)
            .kind(EVAL)
            .await?;

        loop {
            select! {
                sample = subscriber.receiver().next().fuse() => {

                    match self.process_sample(sample).await {
                        Ok(_) => (),
                        Err(e) => log::error!("Process file storage failed: {:?}",e ),
                    }

                }
                query = queryable.receiver().next().fuse() => {
                    match self.process_query(query).await {
                        Ok(_) => (),
                        Err(e) => log::error!("Process file retrieve failed: {:?}",e ),
                    }
                }
            }
        }
    }

    async fn process_query(&self, query: Option<Query>) -> ZResult<()> {
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

        // match query.selector().path_expr.is_a_path() {
        //     false => zerror!(ZErrorKind::Other {
        //         descr: format!("Malformend query {:?}", query.selector)
        //     }),
        //     _ => Ok(()),
        // }?;

        let query_path = query.selector().key_selector;

        log::debug!("Received query {:?}", query_path);
        let complete_path = extract_complete_file_path(&resource_prefix, query_path)?;

        let resp: Sample = match extract_chunk_number(&complete_path) {
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
                let value = Value::new(metadata.as_bytes().into()).encoding(Encoding::APP_JSON);
                Sample::new(query_path.to_string(), value)
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
                let value = Value::new(data.into()).encoding(Encoding::APP_OCTET_STREAM);
                Sample::new(query_path.to_string(), value)
            }
        };

        query.reply_async(resp).await;
        Ok(())
    }

    async fn process_sample(&self, sample: Option<Sample>) -> ZResult<()> {
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
        log::debug!("Received data from {:?}", sample.res_name);
        match sample.kind {
            SampleKind::Put | SampleKind::Patch => match sample.value.encoding.prefix {
                1 => {
                    //Encoding::APP_OCTET_STREAM => {
                    let data = sample.value.payload.to_vec();
                    log::debug!("Received {:?} bytes", data.len());

                    let path = extract_file_path(&resource_prefix, &sample.res_name)?;
                    let chunk_number = extract_chunk_number(&sample.res_name)?;
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
                5 => {
                    //Encoding::APP_JSON => {
                    let value = String::from_utf8(sample.value.payload.to_vec()).map_err(|e| {
                        zerror2!(ZErrorKind::Other {
                            descr: format!("Malformend metadata {:?}", e)
                        })
                    })?;
                    let metadata = FileMetadata::deserialize(&value)?;
                    let path = metadata.resource_name.clone();
                    let hashed_path = hash_path(&path);
                    let metadata_path = self.config.chunks_dir.join(&hashed_path).join("metadata");

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
            },
            SampleKind::Delete => {
                //We should delete the chunk in this case.
                log::trace!("We should delete the chunk");
                Ok(())
            }
        }
    }
}
