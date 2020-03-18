# `d4` - Dense Depth Data Dump

`d4` is a noval file format used to store quantitative genomics data. 
It's designed for both space and time effcient and feature-rich. 

## Build

### Prerequisites

To build `d4`, Rust toolchain is required. To install Rust toolchain, 
please run the following command and follow the prompt to complete the 
Rust installation.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

`gcc` or `clang` is required to build `htslib` embeded with the `d4` library.
For details, please check the htslib repository.

### Build

Normally, the build step is quite easy. Just

```bash
# For Debug Build
cargo build
# For Release Build
cargo build --release
```

And it will produce the `d4utils` binary which you can find at either
`target/debug/d4utils` or `target/release/d4utils` depends on which build mode
you choose.

For some machines which static linking doesn't work, please export the following environment variable
before build so that it will link htslib dynamically.

```bash
export HTSLIB=dynamic

# Make sure you clean the prior build
cargo clean

# For debug build
cargo build

# For release build
cargo build --release

```
