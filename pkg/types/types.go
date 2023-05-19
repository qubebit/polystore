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
		PartNumber int64  `xml:"PartNumber" json:"PartNumber"`
	}

	// Backend is a generic interface for storage backends
	Storage interface {
		ListWithContext(ctx context.Context, prefix string) (*[]Object, error)
		ReadWithContext(ctx context.Context, path string, writer io.Writer) (int64, error)
		WriteWithContext(ctx context.Context, path string, reader io.Reader, size int64) (int64, error)
		StatWithContext(ctx context.Context, path string) (*Object, error)
		DeleteWithContext(ctx context.Context, path string) error
		MoveWithContext(ctx context.Context, fromPath string, toPath string) error
		MoveToBucketWithContext(ctx context.Context, srcPath, dstPath, dstBucket string) error
		InitiateMultipartUploadWithContext(ctx context.Context, path string) (string, error)
		WriteMultipartWithContext(ctx context.Context, path, uploadID string, partNumber int64, reader io.ReadSeeker, size int64) (int64, *CompletedPart, error)
		CompleteMultipartUploadWithContext(ctx context.Context, path, uploadID string, completedParts []*CompletedPart) error
		AbortMultipartUploadWithContext(ctx context.Context, path, uploadID string) error
	}
)

// HasExtension determines whether or not an object contains a file extension
func (object Object) HasExtension(extension string) bool {
	return filepath.Ext(object.Path) == fmt.Sprintf(".%s", extension)
}
