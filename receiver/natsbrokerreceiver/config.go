package natsbrokerreceiver

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

const defaultMonitoringEndpoint = "http://127.0.0.1:8222"

var supportedMonitoringSchemes = []string{"http", "https"}

type AuthConfig struct {
	Username string              `mapstructure:"username"`
	Password configopaque.String `mapstructure:"password"`

	_ struct{}
}

type Config struct {
	Endpoints          []string               `mapstructure:"endpoint"`
	TLS                configtls.ClientConfig `mapstructure:"tls"`
	Auth               AuthConfig             `mapstructure:"auth"`
	CollectionInterval time.Duration          `mapstructure:"collection_interval"`
	Timeout            time.Duration          `mapstructure:"timeout"`

	_ struct{}
}

var (
	_ component.Config    = (*Config)(nil)
	_ confmap.Unmarshaler = (*Config)(nil)
	_ xconfmap.Validator  = (*Config)(nil)
)

func createDefaultConfig() component.Config {
	return &Config{
		Endpoints:          []string{defaultMonitoringEndpoint},
		CollectionInterval: 10 * time.Second,
		Timeout:            5 * time.Second,
	}
}

func (cfg *Config) Unmarshal(conf *confmap.Conf) error {
	type rawConfig struct {
		Endpoint           any                    `mapstructure:"endpoint"`
		TLS                configtls.ClientConfig `mapstructure:"tls"`
		Auth               AuthConfig             `mapstructure:"auth"`
		CollectionInterval time.Duration          `mapstructure:"collection_interval"`
		Timeout            time.Duration          `mapstructure:"timeout"`
	}

	raw := rawConfig{
		Endpoint:           append([]string(nil), cfg.Endpoints...),
		TLS:                cfg.TLS,
		Auth:               cfg.Auth,
		CollectionInterval: cfg.CollectionInterval,
		Timeout:            cfg.Timeout,
	}

	if err := conf.Unmarshal(&raw); err != nil {
		return err
	}

	endpoints, err := parseMonitoringEndpoints(raw.Endpoint)
	if err != nil {
		return err
	}
	if len(endpoints) == 0 {
		endpoints = []string{defaultMonitoringEndpoint}
	}

	cfg.Endpoints = endpoints
	cfg.TLS = raw.TLS
	cfg.Auth = raw.Auth
	cfg.CollectionInterval = raw.CollectionInterval
	cfg.Timeout = raw.Timeout

	return nil
}

func (cfg *Config) Validate() error {
	if len(cfg.Endpoints) == 0 {
		return errors.New(`requires at least one "endpoint"`)
	}
	for _, endpoint := range cfg.Endpoints {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
		}
		if !slices.Contains(supportedMonitoringSchemes, parsed.Scheme) {
			return fmt.Errorf("unsupported endpoint scheme %q", parsed.Scheme)
		}
		if parsed.Host == "" {
			return fmt.Errorf("invalid endpoint %q: host must not be empty", endpoint)
		}
	}
	if cfg.CollectionInterval <= 0 {
		return errors.New(`"collection_interval" must be greater than zero`)
	}
	if cfg.Timeout <= 0 {
		return errors.New(`"timeout" must be greater than zero`)
	}
	if (cfg.Auth.Username == "") != (cfg.Auth.Password == "") {
		return errors.New(`auth.username and auth.password must be configured together`)
	}
	return nil
}

func parseMonitoringEndpoints(value any) ([]string, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case string:
		if strings.TrimSpace(typed) == "" {
			return nil, nil
		}
		return []string{typed}, nil
	case []string:
		return slices.Clone(typed), nil
	case []any:
		endpoints := make([]string, 0, len(typed))
		for _, entry := range typed {
			endpoint, ok := entry.(string)
			if !ok {
				return nil, fmt.Errorf("endpoint list items must be strings, got %T", entry)
			}
			if strings.TrimSpace(endpoint) == "" {
				continue
			}
			endpoints = append(endpoints, endpoint)
		}
		return endpoints, nil
	default:
		return nil, fmt.Errorf("endpoint must be a string or a list of strings, got %T", value)
	}
}
