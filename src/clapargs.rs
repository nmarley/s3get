use clap::Parser;

/// s3get - download s3 objects in parallel
#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// The s3 bucket from which to fetch objects
    #[arg(long = "bucket")]
    pub s3_bucket: String,
    /// The input file from which to read s3 keys
    #[arg(long)]
    pub keys_file: String,
    /// The number of threads to use (defaults to NUM_CPUS * 8)
    #[arg(long)]
    pub num_threads: Option<usize>,
}
