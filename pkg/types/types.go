package types

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"time"
)

type (
	// Object is a generic representation of a storage object
	Object struct {
		Meta         Metadata
		Path         string
		LastModified time.Time
	}

	// Metadata represents the meta information of the object
	// includes object name , object version , etc...
	Metadata struct {
		Name    string
		Version string
	}

	// ObjectSliceDiff provides information on what has changed since last calling ListObjects
	ObjectSliceDiff struct {
		Change  bool
		Removed []Object
		Added   []Object
		Updated []Object
	}

	// CompletedPart represents a part that has been uploaded
	CompletedPart struct {
		ETag       string `xml:"ETag" json:"ETag"`
		Size       int64  `xml:"Size" json:"Size"`
		PartNumber int32  `xml:"PartNumber" json:"PartNumber"`
	}

	ListOpts struct {
		Path string
	}

	ListResult struct {
		Objects []Object
	}

	DownloadOpts struct {
		Path   string
		Writer io.WriterAt
		Start  int64
		End    int64
	}

	DownloadResult struct {
		BytesWritten int64
	}

	UploadOpts struct {
		Path   string
		Reader io.Reader
	}

	StatOpts struct {
		Path string
	}

	StatResult struct {
		Object Object
	}

	DeleteOpts struct {
		Path string
	}

	MoveOpts struct {
		FromPath string
		ToPath   string
	}

	CreateMultipartUploadOpts struct {
		Path string
	}

	CreateMultipartUploadResult struct {
		UploadID string
	}

	UploadPartOpts struct {
		Path       string
		UploadID   string
		PartNumber int32
		Reader     io.ReadSeeker
		Size       int64
	}

	UploadPartResult struct {
		CompletedPart CompletedPart
	}

	CompleteMultipartUploadOpts struct {
		Path           string
		UploadID       string
		CompletedParts []CompletedPart
	}

	AbortMultipartUploadOpts struct {
		Path     string
		UploadID string
	}

	GeneratePresignedURLOpts struct {
		Path       string
		Expires    time.Duration
		UploadID   string
		PartNumber int32
	}

	GeneratePresignedURLResult struct {
		URL string
	}

	// Storage is a generic interface for storage backends
	Storage interface {
		List(ctx context.Context, opts *ListOpts) (*ListResult, error)
		Download(ctx context.Context, opts *DownloadOpts) (*DownloadResult, error)
		Upload(ctx context.Context, opts *UploadOpts) error
		Stat(ctx context.Context, opts *StatOpts) (*StatResult, error)
		Delete(ctx context.Context, opts *DeleteOpts) error
		Move(ctx context.Context, opts *MoveOpts) error
		CreateMultipartUpload(ctx context.Context, opts *CreateMultipartUploadOpts) (*CreateMultipartUploadResult, error)
		UploadPart(ctx context.Context, opts *UploadPartOpts) (*UploadPartResult, error)
		CompleteMultipartUpload(ctx context.Context, opts *CompleteMultipartUploadOpts) error
		AbortMultipartUpload(ctx context.Context, opts *AbortMultipartUploadOpts) error
		GeneratePresignedURL(ctx context.Context, opts *GeneratePresignedURLOpts) (*GeneratePresignedURLResult, error)
	}
)

// HasExtension determines whether or not an object contains a file extension
func (object Object) HasExtension(extension string) bool {
	return filepath.Ext(object.Path) == fmt.Sprintf(".%s", extension)
}
