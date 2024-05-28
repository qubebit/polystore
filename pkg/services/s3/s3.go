package s3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	pathutil "path"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/backdrop-run/polystore/pkg/helpers"
	"github.com/backdrop-run/polystore/pkg/services"
	"github.com/backdrop-run/polystore/pkg/types"
)

type (
	// Options is a struct for S3 options
	Options struct {
		Endpoint   string
		Prefix     string
		Region     string
		AccessKey  string
		SecretKey  string
		BucketName string
		SSE        string
	}

	// Backend is a storage backend for S3
	Backend struct {
		Bucket     string
		Client     *s3.Client
		Presign    *s3.PresignClient
		Downloader *manager.Downloader
		Prefix     string
		Uploader   *manager.Uploader
		SSE        string
	}
)

func init() {
	services.Register("s3", new)
}

func new(uri *url.URL) (types.Storage, error) {
	opts := Options{
		Endpoint:   uri.Query().Get("endpoint"),
		Prefix:     filepath.Clean(uri.Path),
		Region:     uri.Query().Get("region"),
		AccessKey:  uri.Query().Get("accessKey"),
		SecretKey:  uri.Query().Get("secretKey"),
		BucketName: uri.Host,
		SSE:        uri.Query().Get("sse"),
	}

	return New(opts)
}

// New creates a new S3 backend
func New(opts Options) (*Backend, error) {
	credentials := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(opts.AccessKey, opts.SecretKey, ""))

	client := http.DefaultClient
	if os.Getenv("AWS_INSECURE_SKIP_VERIFY") == "true" {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
		client = &http.Client{Transport: tr}
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithHTTPClient(client),
		config.WithCredentialsProvider(credentials),
		config.WithRegion(opts.Region),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: opts.Endpoint, SigningRegion: opts.Region}, nil
			})),
	)
	if err != nil {
		return nil, err
	}

	service := s3.NewFromConfig(cfg)
	return &Backend{
		Bucket:     opts.BucketName,
		Client:     service,
		Presign:    s3.NewPresignClient(service),
		Downloader: manager.NewDownloader(service),
		Prefix:     helpers.CleanPrefix(opts.Prefix),
		Uploader:   manager.NewUploader(service),
		SSE:        opts.SSE,
	}, nil
}

// List lists all objects in a S3 bucket, at prefix
// Note: This function does not handle pagination and will return a maximum of 1000 objects.
// If there are more than 1000 objects with the specified prefix, consider using pagination to retrieve all objects.
func (b *Backend) List(ctx context.Context, prefix string) (*[]types.Object, error) {
	var objects []types.Object
	prefix = pathutil.Join(b.Prefix, prefix)
	s3Input := &s3.ListObjectsV2Input{
		Bucket: aws.String(b.Bucket),
		Prefix: aws.String(prefix),
	}

	s3Result, err := b.Client.ListObjectsV2(ctx, s3Input)
	if err != nil {
		return nil, err
	}

	for _, obj := range s3Result.Contents {
		path := helpers.RemovePrefixFromObjectPath(prefix, *obj.Key)
		if helpers.ObjectPathIsInvalid(path) {
			continue
		}
		object := types.Object{
			Path:         path,
			LastModified: *obj.LastModified,
		}
		objects = append(objects, object)
	}

	return &objects, nil
}

// Download downloads an object from S3 bucket, at given path
func (b *Backend) Download(ctx context.Context, path string, writer io.WriterAt, start int64, end int64) (int64, error) {
	rangeStr := fmt.Sprintf("bytes=%d-", start)
	if end != 0 {
		rangeStr = fmt.Sprintf("bytes=%d-%d", start, end)
	}

	s3Input := &s3.GetObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
		Range:  aws.String(rangeStr),
	}

	bytesDownloaded, err := b.Downloader.Download(ctx, writer, s3Input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
			return 0, fmt.Errorf("no such key %s: %w", path, err)
		}

		return 0, fmt.Errorf("failed to download object: %w", err)
	}

	return bytesDownloaded, nil
}

// Stat returns information about an object in a S3 bucket, at given path
func (b *Backend) Stat(ctx context.Context, path string) (*types.Object, error) {
	s3Input := &s3.GetObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
	}

	s3Result, err := b.Client.GetObject(ctx, s3Input)
	if err != nil {
		return nil, err
	}

	return &types.Object{
		Path:         path,
		LastModified: *s3Result.LastModified,
	}, nil
}

// Upload uploads an object to a S3 bucket, at prefix.
// Will intelligently buffering large files into smaller chunks and sending them in parallel across multiple goroutines
func (b *Backend) Upload(ctx context.Context, path string, reader io.Reader) error {
	s3Input := &s3.PutObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
		Body:   reader,
	}

	if b.SSE != "" {
		s3Input.ServerSideEncryption = s3types.ServerSideEncryption(b.SSE)
	}

	if _, err := b.Uploader.Upload(ctx, s3Input); err != nil {
		return err
	}

	return nil
}

// DeleteObject removes an object from a S3 bucket, at prefix
func (b *Backend) Delete(ctx context.Context, path string) error {
	s3Input := &s3.DeleteObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
	}
	_, err := b.Client.DeleteObject(ctx, s3Input)
	return err
}

// MoveObject moves an object from one path to another within a S3 bucket
func (b *Backend) Move(ctx context.Context, fromPath string, toPath string) error {
	// First, copy the source file to the destination path
	copyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(b.Bucket),
		CopySource: aws.String(url.PathEscape(pathutil.Join(b.Bucket, b.Prefix, fromPath))),
		Key:        aws.String(pathutil.Join(b.Prefix, toPath)),
	}
	if b.SSE != "" {
		copyInput.ServerSideEncryption = s3types.ServerSideEncryption(b.SSE)
	}

	_, err := b.Client.CopyObject(ctx, copyInput)
	if err != nil {
		return fmt.Errorf("failed to copy file from %s to %s: %w", fromPath, toPath, err)
	}

	// If the copy is successful, delete the source file
	err = b.Delete(ctx, fromPath)
	if err != nil {
		return fmt.Errorf("failed to delete source file %s after copying: %w", fromPath, err)
	}

	return nil
}

// CreateMultipartUpload initiates a multipart upload.
func (b *Backend) CreateMultipartUpload(ctx context.Context, path string) (string, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
	}

	if b.SSE != "" {
		input.ServerSideEncryption = s3types.ServerSideEncryption(b.SSE)
	}

	output, err := b.Client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	return *output.UploadId, nil
}

// UploadPart writes a part of a multipart upload
func (b *Backend) UploadPart(ctx context.Context, path, uploadID string, partNumber int32, reader io.ReadSeeker, size int64) (int64, *types.CompletedPart, error) {
	input := &s3.UploadPartInput{
		Bucket:        aws.String(b.Bucket),
		Key:           aws.String(pathutil.Join(b.Prefix, path)),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int32(partNumber),
		ContentLength: aws.Int64(size),
		Body:          reader,
	}

	output, err := b.Client.UploadPart(ctx, input)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to upload part: %w", err)
	}

	return size, &types.CompletedPart{
		ETag:       *output.ETag,
		Size:       size,
		PartNumber: partNumber,
	}, nil
}

// CompleteMultipartUpload completes a multipart upload
func (b *Backend) CompleteMultipartUpload(ctx context.Context, path, uploadID string, completedParts []types.CompletedPart) error {
	s3CompletedParts := make([]s3types.CompletedPart, len(completedParts))
	for i, part := range completedParts {
		s3CompletedParts[i] = s3types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(part.PartNumber),
		}
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(b.Bucket),
		Key:      aws.String(pathutil.Join(b.Prefix, path)),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: s3CompletedParts,
		},
	}

	_, err := b.Client.CompleteMultipartUpload(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}

// AbortMultipartUpload aborts the multipart upload
func (b *Backend) AbortMultipartUpload(ctx context.Context, path, uploadID string) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(b.Bucket),
		Key:      aws.String(pathutil.Join(b.Prefix, path)),
		UploadId: aws.String(uploadID),
	}

	_, err := b.Client.AbortMultipartUpload(ctx, input)
	if err != nil {
		// AbortMultipartUpload is non-idempotent in s3, we need to omit `NoSuchUpload` error for compatibility.
		// AWS S3 is inconsistent in its behavior, it returns 204 No Content within 24 hours and returns 404 Not Found after 24hours.
		e := &s3types.NoSuchUpload{}
		if errors.As(err, &e) {
			err = nil
		}
	}
	if err != nil {
		return err
	}

	return nil
}

// GeneratePresignedURL generates a presigned URL for an object in a S3 bucket
func (b *Backend) GeneratePresignedURL(ctx context.Context, path string, expires time.Duration, uploadID string, partNumber int32) (string, error) {
	if uploadID == "" {
		return b.generatePresignedURL(ctx, path, expires)
	}

	return b.generatePresignedPartURL(ctx, path, uploadID, partNumber, expires)
}

// generatePresignedURL generates a presigned URL for an object in a S3 bucket
func (b *Backend) generatePresignedURL(ctx context.Context, path string, expires time.Duration) (string, error) {
	req, err := b.Presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
	}, func(o *s3.PresignOptions) {
		o.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	return req.URL, nil
}

// generatePresignedPartURL generates a presigned URL for a part of a multipart upload
func (b *Backend) generatePresignedPartURL(ctx context.Context, path string, uploadID string, partNumber int32, expires time.Duration) (string, error) {
	req, err := b.Presign.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(b.Bucket),
		Key:        aws.String(pathutil.Join(b.Prefix, path)),
		PartNumber: aws.Int32(partNumber),
		UploadId:   aws.String(uploadID),
	}, func(o *s3.PresignOptions) {
		o.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	return req.URL, nil
}
