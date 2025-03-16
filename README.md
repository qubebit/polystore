# PolyStore

PolyStore is a flexible and extensible Go library for unifying and abstracting different storage backends. 

[![Github tag](https://badgen.net/github/tag/qubebit/polystore)](https://github.com/qubebit/polystore/tags)
[![Go Doc](https://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)](https://pkg.go.dev/github.com/qubebit/polystore)
[![Go Report Card](https://goreportcard.com/badge/github.com/qubebit/polystore)](https://goreportcard.com/report/github.com/qubebit/polystore)
[![MIT license](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/qubebit/polystore/blob/master/LICENSE)

It supports various storage services including:

- [Amazon S3 and compatible services](./pkg/services/s3/README.md)
- [Local filesystem](./pkg/services/fs/README.md)

## Features

- Allows operations to be performed without worrying about the specifics of the storage backend.
- Extensible design allows more storage backends to be added easily.

## Installation

```bash
go get github.com/qubebit/polystore
```

## Usage

First, import the storage backends you want to use:

```go
import (
    _ "github.com/qubebit/polystore/pkg/services/fs"
    _ "github.com/qubebit/polystore/pkg/services/s3"

    "github.com/qubebit/polystore/pkg/services"
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