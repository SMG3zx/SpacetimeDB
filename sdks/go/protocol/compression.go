package protocol

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"

	"github.com/andybalholm/brotli"
)

// Compression identifies the compression algorithm used for a server message.
type Compression uint8

const (
	CompressionNone   Compression = 0
	CompressionBrotli Compression = 1
	CompressionGzip   Compression = 2
)

// ErrUnknownCompression is returned when the compression tag is unrecognised.
var ErrUnknownCompression = errors.New("protocol: unknown compression tag")

// DecompressServerMessage reads the compression tag from the first byte of a
// raw WebSocket binary frame and decompresses the remainder.
//
// Frame layout:
//
//	byte 0: compression tag (0=none, 1=brotli, 2=gzip)
//	bytes 1..N: payload
func DecompressServerMessage(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("protocol: empty server message")
	}

	tag := Compression(data[0])
	payload := data[1:]

	switch tag {
	case CompressionNone:
		return payload, nil

	case CompressionBrotli:
		r := brotli.NewReader(bytes.NewReader(payload))
		out, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: brotli decompress: %w", err)
		}
		return out, nil

	case CompressionGzip:
		r, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, fmt.Errorf("protocol: gzip reader: %w", err)
		}
		out, err := io.ReadAll(r)
		if err != nil {
			return nil, fmt.Errorf("protocol: gzip decompress: %w", err)
		}
		return out, nil

	default:
		return nil, fmt.Errorf("%w: %d", ErrUnknownCompression, tag)
	}
}

// String returns the compression name.
func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "None"
	case CompressionBrotli:
		return "Brotli"
	case CompressionGzip:
		return "Gzip"
	default:
		return fmt.Sprintf("Unknown(%d)", c)
	}
}
