//! S3-compatible object storage client construction.
//!
//! Points at MinIo locally (via S3_ENDPOINT), swappable to real AWS
//! S3 later with no code change beyond removing the endpoint override.
pub async fn build_s3_client() -> aws_sdk_s3::Client {
    let s3_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(std::env::var("S3_ENDPOINT").expect("S3_ENDPOINT must be set"))
        .region(aws_sdk_s3::config::Region::new(
            std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        ))
        .credentials_provider(aws_sdk_s3::config::Credentials::new(
            std::env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID must be set"),
            std::env::var("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY must be set"),
            None,
            None,
            "static"
        ))
        .load()
        .await;

    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&s3_config)
            .force_path_style(true)
            .build(),
    )
}
