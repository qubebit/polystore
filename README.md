# PolyStore

PolyStore is a flexible and extensible Go library for unifying and abstracting different storage backends. 

[![Github tag](https://badgen.net/github/tag/flowshot-io/polystore)](https://github.com/flowshot-io/polystore/tags)
[![Go Doc](https://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://pkg.go.dev/github.com/flowshot-io/polystore)
[![Go Report Card](https://goreportcard.com/badge/github.com/flowshot-io/polystore)](https://goreportcard.com/report/github.com/flowshot-io/polystore)
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/flowshot-io/polystore/blob/master/LICENSE)

It supports various storage services including:

- [Amazon S3 and compatible services](./pkg/storage/services/s3/README.md)
- [Local filesystem](./pkg/storage/services/fs/README.md)

## Features

- Allows operations to be performed without worrying about the specifics of the storage backend.
- Extensible design allows more storage backends to be added easily.

## Installation

```bash
go get github.com/flowshot-io/polyStore
```

## Usage

First, import the storage backends you want to use:

```go
import (
	_ "github.com/flowshot-io/polyStore/pkg/storage/services/fs"
	_ "github.com/flowshot-io/polyStore/pkg/storage/services/s3"

    "github.com/flowshot-io/polyStore/pkg/storage/services"
)
```

Then, you can create a storage instance using a connection string:

```go
connectionString := "s3://myBucketName/my/prefix?endpoint=s3.amazonaws.com&region=region&accessKey=accessKey&secretKey=secretKey&sse=sse"
storage, err := services.New(connectionString)
if err != nil {
	// handle error
}
```

## Adding new backends

To add a new backend, you need to implement the `types.Storage` interface and register a factory function for your backend in the `services` package. See the existing backends for examples.

## License

MIT