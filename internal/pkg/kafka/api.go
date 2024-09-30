package kafka

import (
	"fmt"
)

type ApiKeyVersionRange struct {
	Min int16
	Max int16
}

type ApiKey int16

func (key *ApiKey) VersionRange() (*ApiKeyVersionRange, error) {
	v, ok := versions[*key]
	if !ok {
		return nil, fmt.Errorf("Invalid API key: %d", key)
	}

	return &v, nil
}

const (
	ApiKeyFetch       ApiKey = 1
	ApiKeyApiVersions ApiKey = 18
)

var versions = map[ApiKey]ApiKeyVersionRange{
	ApiKeyFetch:       {Min: 1, Max: 16},
	ApiKeyApiVersions: {Min: 1, Max: 4},
}

const (
	CodeInvalidApiVersion int16 = 35
)

type Request struct {
	Length        int32
	ApiKey        ApiKey
	ApiVersion    int16
	CorrelationId int32
	Body          []byte
}

func (req *Request) Validate() int16 {
	v := versions[req.ApiKey]
	if req.ApiVersion < v.Min || req.ApiVersion > v.Max {
		return CodeInvalidApiVersion
	}

	return 0
}
