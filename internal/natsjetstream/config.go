package natsjetstream

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap/xconfmap"
)

const (
	DefaultEndpoint      = "nats://127.0.0.1:4222"
	DefaultInboxPrefix   = "_INBOX"
	DefaultSubjectPrefix = "otlp"
)

var supportedSchemes = []string{"nats", "tls", "ws", "wss"}

type Signal string

const (
	SignalTraces  Signal = "traces"
	SignalMetrics Signal = "metrics"
	SignalLogs    Signal = "logs"
)

func (s Signal) SubjectSuffix() string {
	return "v1." + string(s)
}

func (s Signal) String() string {
	return string(s)
}

func SignalFromSubject(subject string) (Signal, bool) {
	switch {
	case strings.HasSuffix(subject, "."+SignalTraces.SubjectSuffix()):
		return SignalTraces, true
	case strings.HasSuffix(subject, "."+SignalMetrics.SubjectSuffix()):
		return SignalMetrics, true
	case strings.HasSuffix(subject, "."+SignalLogs.SubjectSuffix()):
		return SignalLogs, true
	default:
		return "", false
	}
}

type AuthTLSConfig struct {
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
	CAFile   string `mapstructure:"ca_file"`

	_ struct{}
}

type AuthConfig struct {
	Token     configopaque.String `mapstructure:"token"`
	Username  string              `mapstructure:"username"`
	Password  configopaque.String `mapstructure:"password"`
	TLS       AuthTLSConfig       `mapstructure:"tls"`
	NKey      configopaque.String `mapstructure:"nkey"`
	JWT       configopaque.String `mapstructure:"jwt"`
	CredsPath string              `mapstructure:"creds_path"`

	_ struct{}
}

type ClientConfig struct {
	Endpoints         []string              `mapstructure:"endpoint"`
	TLS               configtls.ClientConfig `mapstructure:"tls"`
	Compression       string                `mapstructure:"compression"`
	CompressionParams map[string]any        `mapstructure:"compression_params"`
	ProxyURL          string                `mapstructure:"proxy_url"`
	InboxPrefix       string                `mapstructure:"inbox_prefix"`
	Auth              AuthConfig            `mapstructure:"auth"`

	_ struct{}
}

var (
	_ component.Config   = (*ClientConfig)(nil)
	_ xconfmap.Validator = (*ClientConfig)(nil)
)

func NewDefaultClientConfig() ClientConfig {
	return ClientConfig{
		Endpoints:   []string{DefaultEndpoint},
		InboxPrefix: DefaultInboxPrefix,
	}
}

func (cfg *ClientConfig) Validate() error {
	if len(cfg.Endpoints) == 0 {
		return errors.New(`requires at least one "endpoint"`)
	}

	transportClass := ""
	for _, endpoint := range cfg.Endpoints {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
		}
		if !slices.Contains(supportedSchemes, parsed.Scheme) {
			return fmt.Errorf("unsupported endpoint scheme %q", parsed.Scheme)
		}

		currentClass := "tcp"
		if parsed.Scheme == "ws" || parsed.Scheme == "wss" {
			currentClass = "ws"
		}
		if transportClass == "" {
			transportClass = currentClass
		} else if currentClass != transportClass {
			return errors.New("cannot mix websocket endpoints with nats/tls endpoints in one configuration")
		}
	}

	if cfg.ProxyURL != "" && transportClass != "ws" {
		return errors.New(`"proxy_url" is only supported for ws:// and wss:// endpoints`)
	}

	configuredAuthMethods := 0
	if cfg.Auth.Token != "" {
		configuredAuthMethods++
	}
	if cfg.Auth.Username != "" || cfg.Auth.Password != "" {
		if cfg.Auth.Username == "" || cfg.Auth.Password == "" {
			return errors.New("username/password authentication requires both fields")
		}
		configuredAuthMethods++
	}
	if cfg.Auth.NKey != "" {
		configuredAuthMethods++
	}
	if cfg.Auth.JWT != "" {
		configuredAuthMethods++
	}
	if cfg.Auth.CredsPath != "" {
		configuredAuthMethods++
	}
	if configuredAuthMethods > 1 {
		return errors.New("authentication methods are mutually exclusive")
	}

	if cfg.InboxPrefix == "" {
		cfg.InboxPrefix = DefaultInboxPrefix
	}

	return nil
}

func Subject(prefix string, signal Signal) string {
	trimmedPrefix := strings.TrimSuffix(strings.TrimSpace(prefix), ".")
	if trimmedPrefix == "" {
		trimmedPrefix = DefaultSubjectPrefix
	}
	return trimmedPrefix + "." + signal.SubjectSuffix()
}

func CompressPayload(payload []byte, compression string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(compression)) {
	case "", "none":
		return payload, nil
	case "gzip":
		var buffer bytes.Buffer
		writer := gzip.NewWriter(&buffer)
		if _, err := writer.Write(payload); err != nil {
			return nil, fmt.Errorf("gzip payload: %w", err)
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("close gzip payload: %w", err)
		}
		return buffer.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported compression %q", compression)
	}
}

func DecompressPayload(payload []byte, compression string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(compression)) {
	case "", "none":
		return payload, nil
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, fmt.Errorf("open gzip payload: %w", err)
		}
		defer reader.Close()

		decoded, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("read gzip payload: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported compression %q", compression)
	}
}

func ParseEndpoints(value any) ([]string, error) {
	return parseEndpoints(value)
}

func parseEndpoints(value any) ([]string, error) {
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

func Connect(ctx context.Context, cfg ClientConfig) (*nats.Conn, jetstream.JetStream, error) {
	options := make([]nats.Option, 0, 8)
	options = append(options, nats.Name("otelcol-nats"))
	if cfg.InboxPrefix != "" {
		options = append(options, nats.CustomInboxPrefix(cfg.InboxPrefix))
	}
	if cfg.ProxyURL != "" {
		options = append(options, nats.ProxyPath(cfg.ProxyURL))
	}
	if cfg.Auth.Token != "" {
		options = append(options, nats.Token(string(cfg.Auth.Token)))
	}
	if cfg.Auth.Username != "" {
		options = append(options, nats.UserInfo(cfg.Auth.Username, string(cfg.Auth.Password)))
	}
	if cfg.Auth.CredsPath != "" {
		options = append(options, nats.UserCredentials(cfg.Auth.CredsPath))
	}
	if cfg.Auth.NKey != "" {
		nkeyOption, err := nats.NkeyOptionFromSeed(string(cfg.Auth.NKey))
		if err != nil {
			return nil, nil, fmt.Errorf("load nkey seed: %w", err)
		}
		options = append(options, nkeyOption)
	}
	if cfg.Auth.JWT != "" {
		return nil, nil, errors.New("jwt authentication is not wired yet; use creds or nkey for now")
	}

	connection, err := nats.Connect(strings.Join(cfg.Endpoints, ","), options...)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to nats: %w", err)
	}

	js, err := jetstream.New(connection)
	if err != nil {
		connection.Close()
		return nil, nil, fmt.Errorf("create jetstream client: %w", err)
	}

	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= 0 {
			connection.Close()
			return nil, nil, context.DeadlineExceeded
		}
	}

	return connection, js, nil
}