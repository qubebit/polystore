package s3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
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
		PathStyle  bool
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
		PathStyle:  uri.Query().Get("pathStyle") == "true",
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

	service := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = opts.PathStyle
	})
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

// List lists all objects in a S3 bucket, at path
// Note: This function does not handle pagination and will return a maximum of 1000 objects.
// If there are more than 1000 objects with the specified path, consider using pagination to retrieve all objects.
func (b *Backend) List(ctx context.Context, opts *types.ListOpts) (*types.ListResult, error) {
	var objects []types.Object
	path := pathutil.Join(b.Prefix, opts.Path)
	s3Input := &s3.ListObjectsV2Input{
		Bucket: aws.String(b.Bucket),
		Prefix: aws.String(path),
	}

	s3Result, err := b.Client.ListObjectsV2(ctx, s3Input)
	if err != nil {
		return nil, err
	}

	for _, obj := range s3Result.Contents {
		path := helpers.RemovePrefixFromObjectPath(path, *obj.Key)
		if helpers.ObjectPathIsInvalid(path) {
			continue
		}
		object := types.Object{
			Path:         path,
			LastModified: *obj.LastModified,
		}
		objects = append(objects, object)
	}

	return &types.ListResult{Objects: objects}, nil
}

// Download downloads an object from S3 bucket, at given path
func (b *Backend) Download(ctx context.Context, opts *types.DownloadOpts) (*types.DownloadResult, error) {
	rangeStr := fmt.Sprintf("bytes=%d-", opts.Start)
	if opts.End != 0 {
		rangeStr = fmt.Sprintf("bytes=%d-%d", opts.Start, opts.End)
	}

	s3Input := &s3.GetObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, opts.Path)),
		Range:  aws.String(rangeStr),
	}

	bytesDownloaded, err := b.Downloader.Download(ctx, opts.Writer, s3Input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchKey" {
			return nil, fmt.Errorf("no such key %s: %w", opts.Path, err)
		}

		return nil, fmt.Errorf("failed to download object: %w", err)
	}

	return &types.DownloadResult{BytesWritten: bytesDownloaded}, nil
}

// Stat returns information about an object in a S3 bucket, at given path
func (b *Backend) Stat(ctx context.Context, opts *types.StatOpts) (*types.StatResult, error) {
	s3Input := &s3.GetObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, opts.Path)),
	}

	s3Result, err := b.Client.GetObject(ctx, s3Input)
	if err != nil {
		return nil, err
	}

	return &types.StatResult{
		Object: types.Object{
			Path:         opts.Path,
			LastModified: *s3Result.LastModified,
		},
	}, nil
}

// Upload uploads an object to a S3 bucket, at prefix.
// Will intelligently buffering large files into smaller chunks and sending them in parallel across multiple goroutines
func (b *Backend) Upload(ctx context.Context, opts *types.UploadOpts) error {
	s3Input := &s3.PutObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, opts.Path)),
		Body:   opts.Reader,
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
func (b *Backend) Delete(ctx context.Context, opts *types.DeleteOpts) error {
	s3Input := &s3.DeleteObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, opts.Path)),
	}
	_, err := b.Client.DeleteObject(ctx, s3Input)
	return err
}

// MoveObject moves an object from one path to another within a S3 bucket
func (b *Backend) Move(ctx context.Context, opts *types.MoveOpts) error {
	// First, copy the source file to the destination path
	copyInput := &s3.CopyObjectInput{
		Bucket:     aws.String(b.Bucket),
		CopySource: aws.String(url.PathEscape(pathutil.Join(b.Bucket, b.Prefix, opts.FromPath))),
		Key:        aws.String(pathutil.Join(b.Prefix, opts.ToPath)),
	}
	if b.SSE != "" {
		copyInput.ServerSideEncryption = s3types.ServerSideEncryption(b.SSE)
	}

	_, err := b.Client.CopyObject(ctx, copyInput)
	if err != nil {
		return fmt.Errorf("failed to copy file from %s to %s: %w", opts.FromPath, opts.FromPath, err)
	}

	// If the copy is successful, delete the source file
	err = b.Delete(ctx, &types.DeleteOpts{
		Path: opts.FromPath,
	})
	if err != nil {
		return fmt.Errorf("failed to delete source file %s after copying: %w", opts.FromPath, err)
	}

	return nil
}

// CreateMultipartUpload initiates a multipart upload.
func (b *Backend) CreateMultipartUpload(ctx context.Context, opts *types.CreateMultipartUploadOpts) (*types.CreateMultipartUploadResult, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, opts.Path)),
	}

	if b.SSE != "" {
		input.ServerSideEncryption = s3types.ServerSideEncryption(b.SSE)
	}

	output, err := b.Client.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	return &types.CreateMultipartUploadResult{
		UploadID: *output.UploadId,
	}, nil
}

// UploadPart writes a part of a multipart upload
func (b *Backend) UploadPart(ctx context.Context, opts *types.UploadPartOpts) (*types.UploadPartResult, error) {
	input := &s3.UploadPartInput{
		Bucket:        aws.String(b.Bucket),
		Key:           aws.String(pathutil.Join(b.Prefix, opts.Path)),
		UploadId:      aws.String(opts.UploadID),
		PartNumber:    aws.Int32(opts.PartNumber),
		ContentLength: aws.Int64(opts.Size),
		Body:          opts.Reader,
	}

	output, err := b.Client.UploadPart(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to upload part: %w", err)
	}

	return &types.UploadPartResult{
		CompletedPart: types.CompletedPart{
			ETag:       *output.ETag,
			Size:       opts.Size,
			PartNumber: opts.PartNumber,
		},
	}, nil
}

// CompleteMultipartUpload completes a multipart upload
func (b *Backend) CompleteMultipartUpload(ctx context.Context, opts *types.CompleteMultipartUploadOpts) error {
	s3CompletedParts := make([]s3types.CompletedPart, len(opts.CompletedParts))
	for i, part := range opts.CompletedParts {
		s3CompletedParts[i] = s3types.CompletedPart{
			ETag:       aws.String(part.ETag),
			PartNumber: aws.Int32(part.PartNumber),
		}
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(b.Bucket),
		Key:      aws.String(pathutil.Join(b.Prefix, opts.Path)),
		UploadId: aws.String(opts.UploadID),
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
func (b *Backend) AbortMultipartUpload(ctx context.Context, opts *types.AbortMultipartUploadOpts) error {
	input := &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(b.Bucket),
		Key:      aws.String(pathutil.Join(b.Prefix, opts.Path)),
		UploadId: aws.String(opts.UploadID),
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
func (b *Backend) GeneratePresignedURL(ctx context.Context, opts *types.GeneratePresignedURLOpts) (*types.GeneratePresignedURLResult, error) {
	if opts.UploadID == "" {
		return b.generatePresignedURL(ctx, opts.Path, opts.Expires)
	}

	return b.generatePresignedPartURL(ctx, opts.Path, opts.UploadID, opts.PartNumber, opts.Expires)
}

// generatePresignedURL generates a presigned URL for an object in a S3 bucket
func (b *Backend) generatePresignedURL(ctx context.Context, path string, expires time.Duration) (*types.GeneratePresignedURLResult, error) {
	req, err := b.Presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.Bucket),
		Key:    aws.String(pathutil.Join(b.Prefix, path)),
	}, func(o *s3.PresignOptions) {
		o.Expires = expires
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	return &types.GeneratePresignedURLResult{
		URL: req.URL,
	}, nil
}

// generatePresignedPartURL generates a presigned URL for a part of a multipart upload
func (b *Backend) generatePresignedPartURL(ctx context.Context, path string, uploadID string, partNumber int32, expires time.Duration) (*types.GeneratePresignedURLResult, error) {
	req, err := b.Presign.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(b.Bucket),
		Key:        aws.String(pathutil.Join(b.Prefix, path)),
		PartNumber: aws.Int32(partNumber),
		UploadId:   aws.String(uploadID),
	}, func(o *s3.PresignOptions) {
		o.Expires = expires
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate presigned URL: %w", err)
	}

	return &types.GeneratePresignedURLResult{
		URL: req.URL,
	}, nil
}
