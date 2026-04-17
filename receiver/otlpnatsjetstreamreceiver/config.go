package otlpnatsjetstreamreceiver

import (
	"errors"
	"strings"
	"time"

	"github.com/m3nowak/otelcol-nats/internal/natsjetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

type StreamConfig struct {
	Name               string `mapstructure:"name"`
	AutodiscoverPrefix string `mapstructure:"autodiscover_prefix"`

	_ struct{}
}

type ConsumerConfig struct {
	Durable   string `mapstructure:"durable"`
	Ephemeral string `mapstructure:"ephemeral"`

	_ struct{}
}

type Config struct {
	natsjetstream.ClientConfig `mapstructure:",squash"`
	Stream                     StreamConfig   `mapstructure:"stream"`
	Consumer                   ConsumerConfig `mapstructure:"consumer"`
	Timeout                    time.Duration  `mapstructure:"timeout"`

	_ struct{}
}

var (
	_ component.Config    = (*Config)(nil)
	_ confmap.Unmarshaler = (*Config)(nil)
	_ xconfmap.Validator  = (*Config)(nil)
)

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig: natsjetstream.NewDefaultClientConfig(),
		Stream: StreamConfig{
			AutodiscoverPrefix: natsjetstream.DefaultSubjectPrefix + ".>",
		},
		Timeout: 5 * time.Second,
	}
}

func (cfg *Config) Unmarshal(conf *confmap.Conf) error {
	type rawConfig struct {
		Endpoint          any                                 `mapstructure:"endpoint"`
		TLS               configtls.ClientConfig              `mapstructure:"tls"`
		Compression       configcompression.Type              `mapstructure:"compression"`
		CompressionParams configcompression.CompressionParams `mapstructure:"compression_params"`
		ProxyURL          string                              `mapstructure:"proxy_url"`
		InboxPrefix       string                              `mapstructure:"inbox_prefix"`
		Auth              natsjetstream.AuthConfig            `mapstructure:"auth"`
		Stream            StreamConfig                        `mapstructure:"stream"`
		Consumer          ConsumerConfig                      `mapstructure:"consumer"`
		Timeout           time.Duration                       `mapstructure:"timeout"`
	}

	raw := rawConfig{
		Endpoint:          append([]string(nil), cfg.Endpoints...),
		TLS:               cfg.TLS,
		Compression:       cfg.Compression,
		CompressionParams: cfg.CompressionParams,
		ProxyURL:          cfg.ProxyURL,
		InboxPrefix:       cfg.InboxPrefix,
		Auth:              cfg.Auth,
		Stream:            cfg.Stream,
		Consumer:          cfg.Consumer,
		Timeout:           cfg.Timeout,
	}

	if err := conf.Unmarshal(&raw); err != nil {
		return err
	}

	endpoints, err := natsjetstream.ParseEndpoints(raw.Endpoint)
	if err != nil {
		return err
	}
	if len(endpoints) == 0 {
		endpoints = []string{natsjetstream.DefaultEndpoint}
	}

	cfg.Endpoints = endpoints
	cfg.TLS = raw.TLS
	cfg.Compression = raw.Compression
	cfg.CompressionParams = raw.CompressionParams
	cfg.ProxyURL = raw.ProxyURL
	cfg.InboxPrefix = raw.InboxPrefix
	cfg.Auth = raw.Auth
	cfg.Stream = raw.Stream
	cfg.Consumer = raw.Consumer
	cfg.Timeout = raw.Timeout

	return nil
}

func (cfg *Config) Validate() error {
	if err := cfg.ClientConfig.Validate(); err != nil {
		return err
	}
	if cfg.Consumer.Durable != "" && cfg.Consumer.Ephemeral != "" {
		return errors.New("consumer.durable and consumer.ephemeral are mutually exclusive")
	}
	if cfg.Stream.Name == "" && cfg.Stream.AutodiscoverPrefix == "" {
		return errors.New("receiver requires stream.name or stream.autodiscover_prefix")
	}
	return nil
}

func (cfg *Config) subjectPrefix() string {
	prefix := strings.TrimSpace(cfg.Stream.AutodiscoverPrefix)
	if prefix == "" {
		return natsjetstream.DefaultSubjectPrefix
	}
	prefix = strings.TrimSuffix(prefix, ".>")
	prefix = strings.TrimSuffix(prefix, ".")
	if prefix == "" {
		return natsjetstream.DefaultSubjectPrefix
	}
	return prefix
}
