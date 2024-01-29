# s3get

> Download s3 objects in parallel

Written in Rust, uses asynchronous runtime Tokio w/channels to stream keys from
a file, spawn download workers and download objects to disk. Very immature at
this time, doesn't have many configuration options.

Much faster than awscli for downloading multiple objects from an s3 bucket.

## Build

```sh
cargo build --release
```

## Usage

```sh
s3get --bucket <bucket-name> --keys-file <s3-object-keys.txt> [--num-threads n] [--verbose]
```

## License

This project is licensed under either of

 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   https://opensource.org/licenses/MIT)
 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   https://www.apache.org/licenses/LICENSE-2.0)
