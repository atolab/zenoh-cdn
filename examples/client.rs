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

use async_std::sync::Arc;
use std::convert::TryFrom;
use structopt::StructOpt;
use zenoh::{Properties, Zenoh};
use zenoh_cdn::client::Client;

#[derive(StructOpt, Debug)]
pub struct UploadKind {
    #[structopt(parse(from_os_str), name = "Absolute path of the file to be shared")]
    filename: std::path::PathBuf,
    #[structopt(name = "Path in zenoh for the file")]
    resource_path: String,
}

#[derive(StructOpt, Debug)]
pub struct DownloadKind {
    #[structopt(parse(from_os_str), name = "Absolute path of the destination")]
    destination_path: std::path::PathBuf,
    #[structopt(name = "Path in zenoh for the file")]
    resource_path: String,
}

#[derive(StructOpt, Debug)]
pub enum ClientCLI {
    Upload(UploadKind),
    Download(DownloadKind),
}

#[async_std::main]
async fn main() {
    env_logger::init();

    let args = ClientCLI::from_args();
    log::debug!("Args: {:?}", args);

    let zsession = Arc::new(
        Zenoh::new(
            Properties::from(String::from(
                // "mode=peer;listener=unixsock-stream//tmp/zf-registry.sock,tcp/127.0.0.1:8998",
                "mode=peer",
            ))
            .into(),
        )
        .await
        .unwrap(),
    );
    let client = Client::new(zsession);

    match args {
        ClientCLI::Upload(up) => {
            let resource_name = zenoh::Path::try_from(up.resource_path).unwrap();
            let path = client.upload(&up.filename, &resource_name).await.unwrap();
            println!("File uploaded to {:?}", path);
        }
        ClientCLI::Download(down) => {
            let resource_name = zenoh::Path::try_from(down.resource_path).unwrap();
            let path = client
                .download(&resource_name, &down.destination_path)
                .await
                .unwrap();
            println!("File downloaded to: {:?}", path);
        }
    }
}
