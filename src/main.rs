use aws_sdk_s3::Error;
use clap::Parser;
use futures_util::StreamExt;
use simplelog::*;
use std::path::Path;
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
};

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;

mod clapargs;
mod input_line_streamer;
mod s3agent;

use input_line_streamer::InputLineStreamer;
use s3agent::S3Agent;

// lazy static is used to only retrieve the number of cores once
lazy_static! {
    static ref NUM_CORES: usize = num_cpus::get();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line args
    let args = clapargs::Args::parse();
    // Set up logging
    CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Info,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )])
    .unwrap();

    let bin_name = env!("CARGO_PKG_NAME");
    info!(
        "{} started, pid: {}, num_cores: {}",
        bin_name,
        std::process::id(),
        *NUM_CORES,
    );

    // Create an object to stream lines from a file.
    let lines_reader = InputLineStreamer::new(args.keys_file);

    // Create an object to interact with s3
    let s3agent = S3Agent::new(&args.s3_bucket).await;

    // Set number of worker tasks
    let num_workers = match args.num_threads {
        Some(num) => num,
        // Use 8x the number of cores as the default channel buffer size
        None => *NUM_CORES * 8,
    };

    // Create an mpmc channel for streaming lines from the input file into
    // worker tasks. Bounded in order to handle backpressure
    let (tx, rx) = async_channel::bounded(num_workers);

    // Spawn a task to stream lines from the input file into a channel for
    // worker tasks to read from
    tokio::spawn(async move {
        if let Ok(lines_stream) = lines_reader.stream_lines().await {
            lines_stream
                .for_each(|line| {
                    let tx = tx.clone();
                    async move {
                        if let Ok(line) = line {
                            // Send each line to the channel
                            if tx.send(line).await.is_err() {
                                error!("error sending line to channel");
                            }
                        }
                    }
                })
                .await;
            // Close input channel
            tx.close();
        }
    });

    // Create a channel for sending stats from the workers to the output
    // aggregator task
    let (writer_tx, mut writer_rx) = mpsc::channel::<(String, usize)>(num_workers);

    // Create a oneshot channel for sending the count of objects downloaded from
    // the output aggregator task back to the main task once it's finished
    let (count_tx, count_rx) = oneshot::channel::<(u64, u64)>();

    // Spawn a task to gather stats from the workers and aggregate them
    let output_handle = tokio::spawn(async move {
        // Track number of objects and total bytes downloaded
        let mut count_files: u64 = 0;
        let mut total_bytes: u64 = 0;
        while let Some((_key, bytes_len)) = writer_rx.recv().await {
            // aggregate stats
            total_bytes += bytes_len as u64;
            count_files += 1;
        }
        // Send the aggregate stats back to the main task
        let _ = count_tx.send((count_files, total_bytes));
    });

    // Create a vec to hold the handles for the worker + output tasks
    let mut handles = Vec::with_capacity(num_workers + 1);
    handles.push(output_handle);

    // Spawn num_workers tasks to fetch objects from s3, write them to disk and
    // send results to the output channel
    for _ in 0..num_workers {
        let rx = rx.clone();
        let writer_tx = writer_tx.clone();
        let s3agent = s3agent.clone();
        let handle = tokio::spawn(async move {
            while let Ok(line) = rx.recv().await {
                let key = line.trim();
                // Fetch the object bytes from s3
                match s3agent.get_object(key).await {
                    Ok(bytes) => {
                        let bytes_len = bytes.len();

                        // dbg!(&key);

                        // Create any necessary directories
                        let path = Path::new(&key);
                        if let Some(dir) = path.parent() {
                            fs::create_dir_all(dir).await.unwrap();
                        }

                        // Open the file for writing
                        let mut localfh = fs::OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(true)
                            .open(&key)
                            .await
                            .unwrap();
                        match localfh.write_all(&bytes).await {
                            Ok(_) => {
                                if args.verbose {
                                    info!("wrote {} bytes to file: {}", bytes_len, key);
                                }
                                // Send the results to the output channel for appending
                                // to the outfile
                                if writer_tx.send((key.to_owned(), bytes_len)).await.is_err() {
                                    error!("error sending result back to channel");
                                }
                            }
                            Err(e) => {
                                error!("error writing object '{}' to file: {}", key, e);
                            }
                        }
                    }
                    // note: This cannot actually be caught right now because
                    // the SDK is broken, open issue:
                    // https://github.com/awslabs/aws-sdk-rust/issues/501
                    //
                    // ...so 'til the SDK is fixed, it will fall thru to the
                    // lower err match arm
                    Err(Error::NoSuchBucket(e)) => {
                        // TODO: probably want to abort the entire process here
                        // if no such bucket
                        error!("Got an S3 NoSuchBucket error!! {}", e);
                    }
                    Err(Error::NoSuchKey(e)) => {
                        error!("Got an S3 NoSuchKey error for key: '{}', {}", key, e);
                    }
                    Err(e) => {
                        error!("Got an S3 Error for key: '{}', {}", key, e);
                    }
                };
            }
            // Drop this worker's cloned writer channel tx
            drop(writer_tx);
        });
        // Add to the vec of handles so we can wait for all the workers to
        // finish before exiting the main task
        handles.push(handle);
    }
    // Drop the writer channel tx in the main task, only the workers will use
    // this.
    drop(writer_tx);

    // Wait for all the worker and output tasks to finish
    futures::future::join_all(handles).await;

    // Gather the stats from the output aggregator task
    let (count_files, total_bytes) = match count_rx.await {
        Ok((files, bytes)) => (files.to_string(), bytes.to_string()),
        Err(_e) => ("some".to_string(), "some".to_string()),
    };

    info!(
        "{} finished, downloaded {} objects ({} bytes)",
        bin_name, count_files, total_bytes,
    );

    Ok(())
}
