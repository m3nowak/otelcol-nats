package natsjetstream

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"strings"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"go.opentelemetry.io/collector/config/configcompression"
)

// snappyFramingHeader is the magic header that identifies Snappy framed payloads.
var snappyFramingHeader = []byte{
	0xff, 0x06, 0x00, 0x00,
	0x73, 0x4e, 0x61, 0x50, 0x70, 0x59,
}

func CompressPayload(payload []byte, compression configcompression.Type, params configcompression.CompressionParams) ([]byte, error) {
	if !compression.IsCompressed() {
		return payload, nil
	}

	params = compressionParams(compression, params)

	var (
		buffer bytes.Buffer
		writer io.WriteCloser
		err    error
	)

	switch normalizeCompression(compression) {
	case configcompression.TypeGzip:
		writer, err = gzip.NewWriterLevel(&buffer, int(params.Level))
	case configcompression.TypeZlib, configcompression.TypeDeflate:
		writer, err = zlib.NewWriterLevel(&buffer, int(params.Level))
	case configcompression.TypeSnappy:
		encoded := snappy.Encode(nil, payload)
		return encoded, nil
	case configcompression.TypeSnappyFramed:
		writer = snappy.NewBufferedWriter(&buffer)
	case configcompression.TypeZstd:
		writer, err = zstd.NewWriter(&buffer,
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(int(params.Level))),
		)
	case configcompression.TypeLz4:
		lz4Writer := lz4.NewWriter(&buffer)
		err = lz4Writer.Apply(lz4.ConcurrencyOption(1))
		writer = lz4Writer
	default:
		return nil, fmt.Errorf("unsupported compression %q", compression)
	}
	if err != nil {
		return nil, fmt.Errorf("create %s writer: %w", compression, err)
	}
	if _, err := writer.Write(payload); err != nil {
		return nil, fmt.Errorf("compress %s payload: %w", compression, err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close %s payload: %w", compression, err)
	}
	return buffer.Bytes(), nil
}

func DecompressPayload(payload []byte, compression string) ([]byte, error) {
	switch normalizeCompression(configcompression.Type(compression)) {
	case "":
		return payload, nil
	case configcompression.TypeGzip:
		reader, err := gzip.NewReader(bytes.NewReader(payload))
		return readCompressedPayload(reader, err, "gzip")
	case configcompression.TypeZstd:
		reader, err := zstd.NewReader(bytes.NewReader(payload), zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, fmt.Errorf("open zstd payload: %w", err)
		}
		defer reader.Close()
		decoded, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("read zstd payload: %w", err)
		}
		return decoded, nil
	case configcompression.TypeZlib, configcompression.TypeDeflate:
		reader, err := zlib.NewReader(bytes.NewReader(payload))
		return readCompressedPayload(reader, err, string(normalizeCompression(configcompression.Type(compression))))
	case configcompression.TypeSnappy:
		if len(payload) >= len(snappyFramingHeader) && bytes.Equal(payload[:len(snappyFramingHeader)], snappyFramingHeader) {
			decoded, err := io.ReadAll(snappy.NewReader(bytes.NewReader(payload)))
			if err != nil {
				return nil, fmt.Errorf("read snappy payload: %w", err)
			}
			return decoded, nil
		}
		decoded, err := snappy.Decode(nil, payload)
		if err != nil {
			return nil, fmt.Errorf("read snappy payload: %w", err)
		}
		return decoded, nil
	case configcompression.TypeSnappyFramed:
		decoded, err := io.ReadAll(snappy.NewReader(bytes.NewReader(payload)))
		if err != nil {
			return nil, fmt.Errorf("read x-snappy-framed payload: %w", err)
		}
		return decoded, nil
	case configcompression.TypeLz4:
		decoded, err := io.ReadAll(lz4.NewReader(bytes.NewReader(payload)))
		if err != nil {
			return nil, fmt.Errorf("read lz4 payload: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported compression %q", compression)
	}
}

func readCompressedPayload(reader io.ReadCloser, err error, compression string) ([]byte, error) {
	if err != nil {
		return nil, fmt.Errorf("open %s payload: %w", compression, err)
	}
	defer reader.Close()

	decoded, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read %s payload: %w", compression, err)
	}
	return decoded, nil
}

func compressionParams(compression configcompression.Type, params configcompression.CompressionParams) configcompression.CompressionParams {
	if !compression.IsCompressed() || params.Level != 0 {
		return params
	}
	params.Level = configcompression.DefaultCompressionLevel
	return params
}

func normalizeCompression(compression configcompression.Type) configcompression.Type {
	normalized := configcompression.Type(strings.ToLower(strings.TrimSpace(string(compression))))
	if normalized == "none" {
		return ""
	}
	return normalized
}
