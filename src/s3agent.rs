use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Error};
use tokio::io::AsyncReadExt;

#[derive(Clone, Debug)]
pub struct S3Agent {
    bucket: String,
    client: Client,
}

// see:
// https://docs.aws.amazon.com/sdk-for-rust/latest/dg/rust_s3_code_examples.html

impl S3Agent {
    pub async fn new(bucket: &str) -> Self {
        let config = aws_config::load_from_env().await;
        let client = aws_sdk_s3::Client::new(&config);

        Self {
            bucket: bucket.to_owned(),
            client,
        }
    }

    pub async fn get_object(&self, key: &str) -> Result<Vec<u8>, Error> {
        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let mut data = Vec::new();
        let mut stream = ByteStream::into_async_read(object.body);
        // Read the stream into the vec
        stream.read_to_end(&mut data).await.unwrap();

        Ok(data)
    }
}
