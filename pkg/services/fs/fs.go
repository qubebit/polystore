package fs

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/backdrop-run/polystore/pkg/services"
	"github.com/backdrop-run/polystore/pkg/types"
)

type (
	Options struct {
		WorkingDir string
	}

	Backend struct {
		Root        string
		pathMutexes sync.Map
	}
)

func init() {
	services.Register("fs", new)
}

func new(uri *url.URL) (types.Storage, error) {
	opts := Options{
		WorkingDir: uri.Path,
	}

	return New(opts)
}

// New creates a new filesystem backend
func New(opts Options) (*Backend, error) {
	return &Backend{Root: opts.WorkingDir}, nil
}

// List returns a list of objects in the specified directory
func (fs *Backend) List(ctx context.Context, prefix string) (*[]types.Object, error) {
	mu := fs.getMutexForPath(prefix)
	mu.RLock()
	defer mu.RUnlock()

	var objects []types.Object
	fullPath := filepath.Join(fs.Root, prefix)

	err := filepath.Walk(fullPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		objectPath, _ := filepath.Rel(fs.Root, path)
		object := types.Object{
			Path:         objectPath,
			LastModified: info.ModTime(),
		}
		objects = append(objects, object)
		return nil
	})

	if err != nil {
		return nil, err
	}
	return &objects, nil
}

// Download reads a portion of a file and writes it to the provided writer
func (fs *Backend) Download(ctx context.Context, path string, writer io.WriterAt, start int64, end int64) (int64, error) {
	mu := fs.getMutexForPath(path)
	mu.RLock()
	defer mu.RUnlock()

	fullPath := filepath.Join(fs.Root, path)
	file, err := os.Open(fullPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	// Determine the file size
	fileInfo, err := file.Stat()
	if err != nil {
		return 0, err
	}
	fileSize := fileInfo.Size()

	// Adjust end if it's less than 0 or greater than file size
	if end < 0 || end > fileSize {
		end = fileSize
	}

	if start > fileSize {
		return 0, fmt.Errorf("start position %d is beyond file size %d", start, fileSize)
	}

	// Calculate the number of bytes to read
	length := end - start
	if length < 0 {
		return 0, fmt.Errorf("end position %d is before start position %d", end, start)
	}

	// Move to the start of the desired portion
	_, err = file.Seek(start, 0)
	if err != nil {
		return 0, err
	}

	// Read and write the desired number of bytes
	buf := make([]byte, 4096)
	var totalBytesWritten int64
	for totalBytesWritten < length {
		bytesToRead := int64(len(buf))
		if length-totalBytesWritten < bytesToRead {
			bytesToRead = length - totalBytesWritten
		}

		n, err := file.Read(buf[:bytesToRead])
		if err != nil && err != io.EOF {
			return totalBytesWritten, err
		}
		if n == 0 {
			break
		}

		_, err = writer.WriteAt(buf[:n], start+totalBytesWritten)
		if err != nil {
			return totalBytesWritten, err
		}
		totalBytesWritten += int64(n)
	}

	return totalBytesWritten, nil
}

// Stat returns information about the object at the specified path
func (fs *Backend) Stat(ctx context.Context, path string) (*types.Object, error) {
	mu := fs.getMutexForPath(path)
	mu.RLock()
	defer mu.RUnlock()

	fullPath := filepath.Join(fs.Root, path)
	info, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}

	return &types.Object{
		Path:         path,
		LastModified: info.ModTime(),
	}, nil
}

// Upload writes the contents of the reader to the specified path
func (fs *Backend) Upload(ctx context.Context, path string, reader io.Reader) error {
	mu := fs.getMutexForPath(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := filepath.Join(fs.Root, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), os.ModePerm); err != nil {
		return err
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := io.Copy(file, reader); err != nil {
		return err
	}

	return nil
}

// Delete removes the object at the specified path
func (fs *Backend) Delete(ctx context.Context, path string) error {
	mu := fs.getMutexForPath(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := filepath.Join(fs.Root, path)
	return os.Remove(fullPath)
}

func (fs *Backend) Move(ctx context.Context, fromPath string, toPath string) error {
	srcMutex := fs.getMutexForPath(fromPath)
	dstMutex := fs.getMutexForPath(toPath)

	srcMutex.Lock()
	defer srcMutex.Unlock()
	dstMutex.Lock()
	defer dstMutex.Unlock()

	fromFullPath := filepath.Join(fs.Root, fromPath)
	toFullPath := filepath.Join(fs.Root, toPath)

	dir := filepath.Dir(toFullPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	return os.Rename(fromFullPath, toFullPath)
}

// CreateMultipartUpload creates a new multipart upload session
func (fs *Backend) CreateMultipartUpload(ctx context.Context, path string) (string, error) {
	return "", types.ErrNotSupportByBackend
}

// UploadPart writes a part of a multipart upload
func (fs *Backend) UploadPart(ctx context.Context, path, uploadID string, partNumber int32, reader io.ReadSeeker, size int64) (int64, *types.CompletedPart, error) {
	return size, nil, types.ErrNotSupportByBackend
}

// CompleteMultipartUpload finalizes a multipart upload session
func (fs *Backend) CompleteMultipartUpload(ctx context.Context, path, uploadID string, completedParts []types.CompletedPart) error {
	return types.ErrNotSupportByBackend
}

// AbortMultipartUpload cancels a multipart upload session
func (fs *Backend) AbortMultipartUpload(ctx context.Context, path, uploadID string) error {
	return types.ErrNotSupportByBackend
}

// GeneratePresignedURL generates a URL that can be used to upload a part of a multipart upload
func (fs *Backend) GeneratePresignedURL(ctx context.Context, path string, expires time.Duration, uploadID string, partNumber int32) (string, error) {
	return "", types.ErrNotSupportByBackend
}

func (fs *Backend) getMutexForPath(path string) *sync.RWMutex {
	mu, _ := fs.pathMutexes.LoadOrStore(path, &sync.RWMutex{})
	return mu.(*sync.RWMutex)
}
