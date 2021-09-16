<img src="http://zenoh.io/img/zenoh-dragon-small.png" width="150">

[![CI](https://github.com/atolab/zenoh-cdn/actions/workflows/ci.yml/badge.svg)](https://github.com/Alez87/zenoh-fragmentation-e2e/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-EPL%202.0-blue)](https://choosealicense.com/licenses/epl-2.0/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Zenoh-CDN

Zenoh uses an hop-to-hop fragmentation approach, typical of the Name Data Networking (NDN) concept. Although, generally, this is good approach, in some cases, i.e. when dealing with large files, in particular when intermediate nodes may also consider constraint devices, a different approach may be more suitable in order to minimize the number of fragmentation/reconstrcutions.

See the [zenoh documentation](http://zenoh.io/docs/manual/backends/) for more details.

This library is advisable for sending large files because it allows to fragment and reconstruct as less as it can: at source and at destination.
It's a library that relies on zenoh and shares files on zenoh in order to avoid the h2h fragmentation.


-------------------------------

## How to build it

At first, install [Cargo and Rust](https://doc.rust-lang.org/cargo/getting-started/installation.html).

And then build the library with:

```bash
$ cargo build --release --all-targets
```

-------------------------------

## **Examples of usage**

Start the zenoh-cdn server
```bash
$ RUST_LOG=zenoh_cdn=trace ./target/release/examples/server server-config.yml
```

Send and retrieve a file using the `Client` example

```bash
./target/debug/examples/client upload $(pwd)/zenoh.png "/imgs/zenoh"
```

Retrieve

```bash
./target/debug/examples/client download $(pwd)/zenoh2.png "/imgs/zenoh"
```