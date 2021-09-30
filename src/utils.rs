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

use async_std::fs;
use async_std::fs::File;
use async_std::prelude::*;
use async_std::{fs::OpenOptions, io::SeekFrom};
use memmap2::MmapOptions;
use std::path::Path;
use zenoh_util::core::{ZError, ZErrorKind, ZResult};

pub async fn get_bytes_from_file(
    filename: &Path,
    chunk_number: usize,
    chunk_size: usize,
) -> ZResult<Vec<u8>> {
    log::trace!(
        "Getting the file {:?}, chunk number {}.",
        filename,
        chunk_number
    );
    let mut f = File::open(&filename).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("File not found {:?} {:?}", filename, e)
        })
    })?;

    let metadata = fs::metadata(&filename).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Unable to get metadata for {:?} {:?}", filename, e)
        })
    })?;
    let file_size = metadata.len() as usize;

    let offset: usize = chunk_number * chunk_size;
    let real_offset = f.seek(SeekFrom::Start(offset as u64)).await;
    log::trace!(
        "The offset I'd like is {} and the real offset is {:?}.",
        offset,
        real_offset
    );

    let missing_bytes = file_size - offset;
    let buffer_len: usize = missing_bytes.min(chunk_size);
    log::trace!(
        "File size {}, missing_bytes {}. I create a vector of {} bytes.",
        file_size,
        missing_bytes,
        buffer_len
    );
    let mut buffer = vec![0; buffer_len];
    f.read_exact(&mut buffer).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Unable to read from file {:?} {:?}", filename, e)
        })
    })?;
    Ok(buffer)
}

pub async fn create_dir_if_not_exists(dir: &Path) -> ZResult<()> {
    match async_std::fs::create_dir_all(dir).await {
        Ok(()) => Ok(()),
        Err(e) => match e.kind() {
            async_std::io::ErrorKind::AlreadyExists => Ok(()),
            _ => zenoh_util::zerror!(ZErrorKind::Other {
                descr: format!("Error when creating folder {:?} {:?}", dir, e)
            }),
        },
    }
}

pub async fn write_chunk_file(filename: &Path, content: &[u8]) -> ZResult<()> {
    let mut file = async_std::fs::File::create(filename).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Error when creating file {:?} {:?}", filename, e)
        })
    })?;
    Ok(file.write_all(content).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Error when writing bytes to file {:?} {:?}", filename, e)
        })
    })?)
}

pub async fn write_metadata_file(filename: &Path, metadata: &str) -> ZResult<()> {
    let mut file = async_std::fs::File::create(filename).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Error when creating file {:?} {:?}", filename, e)
        })
    })?;
    Ok(file.write_all(metadata.as_bytes()).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Error when writing bytes to file {:?} {:?}", filename, e)
        })
    })?)
}

pub async fn create_destination_file(filename: &Path, size: u64) -> ZResult<File> {
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(filename)
        .await
        .map_err(|e| {
            zenoh_util::zerror2!(ZErrorKind::Other {
                descr: format!("Unable to create file {:?} {:?}", filename, e)
            })
        })?;

    f.set_len(size).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Unable to allocate space in file {:?} {:?}", filename, e)
        })
    })?;

    Ok(f)
}

pub async fn write_destination_file(
    f: &File,
    src: &[u8],
    chunk_num: usize,
    chunk_size: usize,
) -> ZResult<()> {
    let mut data = unsafe {
        MmapOptions::new().map_mut(f).map_err(|e| {
            zenoh_util::zerror2!(ZErrorKind::Other {
                descr: format!("Unable to access file {:?} {:?}", f, e)
            })
        })?
    };
    let initial_position: usize = chunk_num * chunk_size;
    let final_position: usize = initial_position + src.len();
    log::trace!(
        "Write from position {} to position {}.",
        initial_position,
        final_position
    );
    data[initial_position..final_position].copy_from_slice(src);
    Ok(())
}

pub async fn read_file_to_string(path: &Path) -> ZResult<String> {
    Ok(async_std::fs::read_to_string(path).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Unable to read file {:?} {:?}", path, e)
        })
    })?)
}

pub async fn read_file_to_vec(path: &Path) -> ZResult<Vec<u8>> {
    let mut f = File::open(&path).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Unable to open file {:?} {:?}", path, e)
        })
    })?;
    let metadata = fs::metadata(&path).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!("Unable to read file metadata {:?} {:?}", path, e)
        })
    })?;
    let mut buffer = vec![0; metadata.len() as usize];
    f.read(&mut buffer).await.map_err(|e| {
        zenoh_util::zerror2!(ZErrorKind::Other {
            descr: format!(
                "Buffer overflow when reading data from file {:?} {:?}",
                path, e
            )
        })
    })?;

    Ok(buffer)
}
