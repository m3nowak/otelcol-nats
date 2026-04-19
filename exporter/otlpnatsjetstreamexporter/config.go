package otlpnatsjetstreamexporter

import (
	"fmt"
	"strings"

	"github.com/m3nowak/otelcol-nats/internal/natsjetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configcompression"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/xconfmap"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	natsjetstream.ClientConfig `mapstructure:",squash"`
	TimeoutConfig              exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
	QueueConfig                configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
	RetryConfig                configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
	SubjectPrefix              string                                                   `mapstructure:"subject_prefix"`
	ExpectedStream             string                                                   `mapstructure:"expected_stream"`
	Headers                    map[string]string                                        `mapstructure:"headers"`

	_ struct{}
}

var (
	_ component.Config    = (*Config)(nil)
	_ confmap.Unmarshaler = (*Config)(nil)
	_ xconfmap.Validator  = (*Config)(nil)
)

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig: natsjetstream.ClientConfig{
			Endpoints:   []string{natsjetstream.DefaultEndpoint},
			InboxPrefix: natsjetstream.DefaultInboxPrefix,
			Compression: configcompression.TypeGzip,
		},
		TimeoutConfig: exporterhelper.NewDefaultTimeoutConfig(),
		QueueConfig:   configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		RetryConfig:   configretry.NewDefaultBackOffConfig(),
		SubjectPrefix: natsjetstream.DefaultSubjectPrefix,
		ExpectedStream: "",
		Headers:       map[string]string{},
	}
}

func (cfg *Config) Unmarshal(conf *confmap.Conf) error {
	type rawConfig struct {
		Endpoint          any                                                      `mapstructure:"endpoint"`
		TLS               configtls.ClientConfig                                   `mapstructure:"tls"`
		Compression       configcompression.Type                                   `mapstructure:"compression"`
		CompressionParams configcompression.CompressionParams                      `mapstructure:"compression_params"`
		ProxyURL          string                                                   `mapstructure:"proxy_url"`
		InboxPrefix       string                                                   `mapstructure:"inbox_prefix"`
		Auth              natsjetstream.AuthConfig                                 `mapstructure:"auth"`
		TimeoutConfig     exporterhelper.TimeoutConfig                             `mapstructure:",squash"`
		QueueConfig       configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
		RetryConfig       configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
		SubjectPrefix     string                                                   `mapstructure:"subject_prefix"`
		ExpectedStream    string                                                   `mapstructure:"expected_stream"`
		Headers           map[string]string                                        `mapstructure:"headers"`
	}

	raw := rawConfig{
		Endpoint:          append([]string(nil), cfg.Endpoints...),
		TLS:               cfg.TLS,
		Compression:       cfg.Compression,
		CompressionParams: cfg.CompressionParams,
		ProxyURL:          cfg.ProxyURL,
		InboxPrefix:       cfg.InboxPrefix,
		Auth:              cfg.Auth,
		TimeoutConfig:     cfg.TimeoutConfig,
		QueueConfig:       cfg.QueueConfig,
		RetryConfig:       cfg.RetryConfig,
		SubjectPrefix:     cfg.SubjectPrefix,
		ExpectedStream:    cfg.ExpectedStream,
		Headers:           cfg.Headers,
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
	cfg.TimeoutConfig = raw.TimeoutConfig
	cfg.QueueConfig = raw.QueueConfig
	cfg.RetryConfig = raw.RetryConfig
	cfg.SubjectPrefix = raw.SubjectPrefix
	cfg.ExpectedStream = raw.ExpectedStream
	cfg.Headers = raw.Headers

	return nil
}

func (cfg *Config) Validate() error {
	if err := cfg.ClientConfig.Validate(); err != nil {
		return err
	}
	if cfg.SubjectPrefix == "" {
		return fmt.Errorf(`requires a non-empty "subject_prefix"`)
	}
	cfg.ExpectedStream = strings.TrimSpace(cfg.ExpectedStream)
	return nil
}
