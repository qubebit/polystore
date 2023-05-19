# S3 Storage Backend

The S3 backend provides support for Amazon S3 and compatible services.

## Configuration

To use the S3 backend, you need to specify the following options in your connection string:

- `endpoint`: The endpoint URL of your S3 service.
- `region`: The AWS region.
- `accessKey`: Your AWS access key.
- `secretKey`: Your AWS secret key.
- `sse`: Server Side Encryption setting.

A connection string for the S3 backend looks like this:

```
s3://myBucketName/my/prefix?region=region&accessKey=accessKey&secretKey=secretKey&sse=sse
```

Replace `myBucketName/my/prefix`, `region`, `accessKey`, `secretKey`, and `sse` with your actual parameters.