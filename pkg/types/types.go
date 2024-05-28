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

	CompletedPart struct {
		ETag       string `xml:"ETag" json:"ETag"`
		Size       int64  `xml:"Size" json:"Size"`
		PartNumber int32  `xml:"PartNumber" json:"PartNumber"`
	}

	// Backend is a generic interface for storage backends
	Storage interface {
		List(ctx context.Context, prefix string) (*[]Object, error)
		Download(ctx context.Context, path string, start int64, end int64) (io.ReadCloser, error)
		Upload(ctx context.Context, path string, reader io.Reader, size int64) (int64, error)
		Stat(ctx context.Context, path string) (*Object, error)
		Delete(ctx context.Context, path string) error
		Move(ctx context.Context, fromPath string, toPath string) error
		CreateMultipartUpload(ctx context.Context, path string) (string, error)
		UploadPart(ctx context.Context, path, uploadID string, partNumber int32, reader io.ReadSeeker, size int64) (int64, *CompletedPart, error)
		CompleteMultipartUpload(ctx context.Context, path, uploadID string, completedParts []CompletedPart) error
		AbortMultipartUpload(ctx context.Context, path, uploadID string) error
		GeneratePresignedURL(ctx context.Context, path string, expires time.Duration, uploadID string, partNumber int32) (string, error)
	}
)

// HasExtension determines whether or not an object contains a file extension
func (object Object) HasExtension(extension string) bool {
	return filepath.Ext(object.Path) == fmt.Sprintf(".%s", extension)
}
