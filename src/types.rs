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

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zenoh_util::core::{ZError, ZErrorKind, ZResult};

pub static FILES_KEY: &str = "files";
pub static METADATA_KEY: &str = "metadata";
pub static DEFAULT_ROOT: &str = "/zenohcdn";
pub static SEPARATOR: &str = "/";

pub static DEFAULT_CHUNK_SIZE: usize = 1_048_576; //1MB

#[macro_export]
macro_rules! LIST_FILE_PATH {
    ($prefix:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::types::FILES_KEY,
            "*",
            $crate::types::METADATA_KEY
        )
    };
}

#[macro_export]
macro_rules! GET_FILE_METADATA_PATH {
    ($prefix:expr, $hash:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::types::FILES_KEY,
            $hash,
            $crate::types::METADATA_KEY
        )
    };
}

#[macro_export]
macro_rules! GET_FILE_CHUNK_PATH {
    ($prefix:expr, $hash:expr, $chunk:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::types::FILES_KEY,
            $hash,
            $chunk
        )
    };
}

#[macro_export]
macro_rules! GET_FILE_PATH {
    ($prefix:expr, $hash:expr) => {
        format!("{}/{}/{}", $prefix, $crate::types::FILES_KEY, $hash)
    };
}

#[macro_export]
macro_rules! FILE_CHUNK_PATH {
    ($prefix:expr, $hash:expr, $chunk:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::types::FILES_KEY,
            $hash,
            $chunk
        )
    };
}

#[macro_export]
macro_rules! FILE_METADATA_PATH {
    ($prefix:expr, $hash:expr) => {
        format!("{}/{}/{}", $prefix, $crate::types::FILES_KEY, $hash)
    };
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileMetadata {
    pub filename: String,
    pub checksum: String,
    pub chunk_size: usize,
    pub chunks: usize,
    pub resource_name: String,
    pub size: u64,
}

impl FileMetadata {
    pub fn serialize(&self) -> ZResult<String> {
        serde_json::to_string(self).map_err(|e| {
            zenoh_util::zerror2!(ZErrorKind::Other {
                descr: format!("Error serializing metadata {:?} information {}", self, e)
            })
        })
    }

    pub fn deserialize(serialized: &str) -> ZResult<Self> {
        serde_json::from_str(serialized).map_err(|e| {
            zenoh_util::zerror2!(ZErrorKind::Other {
                descr: format!(
                    "Error deserializing metadata {:?} information {}",
                    serialized, e
                )
            })
        })
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub chunks_dir: std::path::PathBuf,
    pub resource_space: String,
}

pub fn extract_file_path(prefix: &str, path: &str) -> ZResult<String> {
    log::trace!("extract_file_path({:?},{:?}", prefix, path);
    let p = path.strip_prefix(prefix).ok_or_else(|| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: "Unable to get resource name".to_string()
        })
    })?;
    let mut v = p.split('/').collect::<Vec<&str>>();
    v.pop();
    Ok(v.join("/"))
}

pub fn extract_complete_file_path(prefix: &str, path: &str) -> ZResult<String> {
    log::trace!("extract_complete_file_path({:?},{:?}", prefix, path);
    let p = path.strip_prefix(prefix).ok_or_else(|| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: "Unable to get resource name".to_string()
        })
    })?;
    Ok(p.to_string())
}

pub fn extract_chunk_number(path: &str) -> ZResult<usize> {
    let mut v = path.split('/').collect::<Vec<&str>>();
    v.pop()
        .ok_or_else(|| {
            zenoh_util::zerror2!(ZErrorKind::Other {
                descr: "Unable to get chunk_number".to_string()
            })
        })?
        .parse::<usize>()
        .map_err(|e| {
            zenoh_util::zerror2!(ZErrorKind::Other {
                descr: format!("Unable to parse chunk_number {:?}", e)
            })
        })
}

pub fn hash_path(path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path);
    let x = hasher.finalize();
    format!("{:X}", x)
}
