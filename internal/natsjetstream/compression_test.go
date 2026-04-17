package natsjetstream

import (
	"bytes"
	"testing"

	"go.opentelemetry.io/collector/config/configcompression"
)

func TestCompressionRoundTripMatchesCollectorTypes(t *testing.T) {
	t.Parallel()

	payload := []byte("compressed telemetry payload")
	testCases := []struct {
		name        string
		compression configcompression.Type
		header      string
	}{
		{name: "none", compression: ""},
		{name: "gzip", compression: configcompression.TypeGzip, header: "gzip"},
		{name: "zstd", compression: configcompression.TypeZstd, header: "zstd"},
		{name: "zlib", compression: configcompression.TypeZlib, header: "zlib"},
		{name: "deflate", compression: configcompression.TypeDeflate, header: "deflate"},
		{name: "snappy", compression: configcompression.TypeSnappy, header: "snappy"},
		{name: "snappy framed", compression: configcompression.TypeSnappyFramed, header: "x-snappy-framed"},
		{name: "lz4", compression: configcompression.TypeLz4, header: "lz4"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			compressed, err := CompressPayload(payload, tc.compression, configcompression.CompressionParams{})
			if err != nil {
				t.Fatalf("compress payload: %v", err)
			}

			header := tc.header
			if header == "" {
				header = string(tc.compression)
			}

			decoded, err := DecompressPayload(compressed, header)
			if err != nil {
				t.Fatalf("decompress payload: %v", err)
			}
			if !bytes.Equal(decoded, payload) {
				t.Fatalf("unexpected payload: %q", decoded)
			}
		})
	}
}
