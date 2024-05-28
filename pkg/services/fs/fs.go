package fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"

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

func New(opts Options) (*Backend, error) {
	return &Backend{Root: opts.WorkingDir}, nil
}

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

func (fs *Backend) Read(ctx context.Context, path string, start int64, end int64) (io.ReadCloser, error) {
	mu := fs.getMutexForPath(path)
	mu.RLock()
	defer mu.RUnlock()

	fullPath := filepath.Join(fs.Root, path)
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}

	// Check for end < 0, if end is less than 0 we read till the end of the file
	if end < 0 {
		content, err := io.ReadAll(file)
		if err != nil {
			file.Close()
			return nil, err
		}

		// Convert read bytes to a reader and then to an io.ReadCloser
		return io.NopCloser(bytes.NewReader(content[start:])), nil
	}

	// Move to the start of the desired portion
	_, err = file.Seek(start, 0)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Read the desired number of bytes (end-start)
	buf := make([]byte, end-start)
	_, err = file.Read(buf)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Convert read bytes to a reader and then to an io.ReadCloser
	return io.NopCloser(bytes.NewReader(buf)), nil
}

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

func (fs *Backend) Write(ctx context.Context, path string, reader io.Reader, size int64) (int64, error) {
	mu := fs.getMutexForPath(path)
	mu.Lock()
	defer mu.Unlock()

	fullPath := filepath.Join(fs.Root, path)
	if err := os.MkdirAll(filepath.Dir(fullPath), os.ModePerm); err != nil {
		return 0, err
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	return io.Copy(file, reader)
}

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

func (fs *Backend) MoveToBucket(ctx context.Context, srcPath, dstPath, dstBucket string) error {
	srcMutex := fs.getMutexForPath(srcPath)
	dstMutex := fs.getMutexForPath(dstPath)

	srcMutex.Lock()
	defer srcMutex.Unlock()
	dstMutex.Lock()
	defer dstMutex.Unlock()

	srcFullPath := filepath.Join(fs.Root, srcPath)
	dstFullPath := filepath.Join(dstBucket, dstPath)

	dir := filepath.Dir(dstFullPath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	return os.Rename(srcFullPath, dstFullPath)
}

func (fs *Backend) InitiateMultipartUpload(ctx context.Context, path string) (string, error) {
	return "", fmt.Errorf("fileSystemBackend does not support multipart uploads")
}

func (fs *Backend) WriteMultipart(ctx context.Context, path, uploadID string, partNumber int64, reader io.ReadSeeker, size int64) (int64, *types.CompletedPart, error) {
	return size, nil, fmt.Errorf("fileSystemBackend does not support multipart uploads")
}

func (fs *Backend) CompleteMultipartUpload(ctx context.Context, path, uploadID string, completedParts []*types.CompletedPart) error {
	return fmt.Errorf("fileSystemBackend does not support multipart uploads")
}

func (fs *Backend) AbortMultipartUpload(ctx context.Context, path, uploadID string) error {
	return fmt.Errorf("fileSystemBackend does not support multipart uploads")
}

func (fs *Backend) getMutexForPath(path string) *sync.RWMutex {
	mu, _ := fs.pathMutexes.LoadOrStore(path, &sync.RWMutex{})
	return mu.(*sync.RWMutex)
}
