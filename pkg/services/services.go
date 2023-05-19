package services

import (
	"errors"
	"net/url"

	"github.com/flowshot-io/polystore/pkg/types"
)

var (
	backends = make(map[string]func(uri *url.URL) (types.Storage, error), 2)
)

func Register(scheme string, factory func(uri *url.URL) (types.Storage, error)) {
	backends[scheme] = factory
}

func NewStorageFromString(connectionString string) (types.Storage, error) {
	uri, err := url.Parse(connectionString)
	if err != nil {
		return nil, err
	}

	factory := backends[uri.Scheme]
	if factory == nil {
		return nil, errors.New("unsupported scheme: " + uri.Scheme)
	}

	return factory(uri)
}
