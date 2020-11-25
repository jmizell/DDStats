package ddstats

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jmizell/ddstats/client"
)

// Default config values
const (
	DefaultWorkerCount   = 10
	DefaultWorkerBuffer  = 100
	DefaultFlushInterval = time.Second * 60
	DefaultMaxErrorCount = 100
	DefaultNamespace     = "ddstats"
)

// Config environment variables
const (
	EnvWorkerCount          = "DDSTATS_WORKER_COUNT"
	EnvWorkerBuffer         = "DDSTATS_WORKER_BUFFER"
	EnvMetricBuffer         = "DDSTATS_METRIC_BUFFER"
	EnvFlushIntervalSeconds = "DDSTATS_FLUSH_INTERVAL"
	EnvMaxErrorCount        = "DDSTATS_MAX_ERROR_COUNT"
	EnvNamespace            = "DDSTATS_NAMESPACE"
	EnvHost                 = "DDSTATS_HOST"
	EnvTags                 = "DDSTATS_TAGS"
	EnvAPIKey               = "DDSTATS_API_KEY"
)

// Config is required to create an new stats object. A config object can be manually created,
// initialized with defaults using NewConfig, or sourced from environment variables.
//
// Required
//
// An API client is required in order to use stats. Either the API key must be set, or an
// API client can be manually created, and added to the config using WithClient.
type Config struct {
	Namespace            string   `json:"namespace"`      // Namespace is prepended to the name of every metric
	Host                 string   `json:"host"`           // Host to apply to every metric
	Tags                 []string `json:"tags"`           // A global list of tags to append to metrics
	APIKey               string   `json:"api_key"`        // Datadog API key
	FlushIntervalSeconds float64  `json:"flush_interval"` // Interval in seconds to send metrics to Datadog
	WorkerCount          int      `json:"worker_count"`   // Number of workers to process metrics updates
	WorkerBuffer         int      `json:"worker_buffer"`  // Buffer capacity for worker queue
	MetricBuffer         int      `json:"metric_buffer"`  // Global buffer capacity for new metrics not yet assigned a worker
	MaxErrors            int      `json:"max_errors"`     // Max number of flush errors to store

	client client.APIClient
}

// NewConfig creates a new config with default values. The host value is
// sourced from os.Hostname().
func NewConfig() *Config {
	hostname, _ := os.Hostname()
	return &Config{
		Namespace:            DefaultNamespace,
		Host:                 hostname,
		Tags:                 []string{},
		FlushIntervalSeconds: DefaultFlushInterval.Seconds(),
		WorkerCount:          DefaultWorkerCount,
		WorkerBuffer:         DefaultWorkerBuffer,
		MetricBuffer:         DefaultWorkerBuffer * DefaultWorkerCount,
		MaxErrors:            DefaultMaxErrorCount,
	}
}

// FromEnv loads config properties from environment variables. Existing variables in the
// config will be overwritten. Environment variables that are not set, or are empty, will
// be ignored.
//
// Supported variables
//
// DDSTATS_WORKER_COUNT, DDSTATS_WORKER_BUFFER, DDSTATS_METRIC_BUFFER DDSTATS_FLUSH_INTERVAL,
// DDSTATS_MAX_ERROR_COUNT, DDSTATS_NAMESPACE, DDSTATS_HOST, DDSTATS_TAGS, DDSTATS_API_KEY
//
func (c *Config) FromEnv() *Config {

	loadEnvString(&c.Namespace, EnvNamespace)
	loadEnvString(&c.Host, EnvHost)
	loadEnvString(&c.APIKey, EnvAPIKey)
	loadEnvFloat64(&c.FlushIntervalSeconds, EnvFlushIntervalSeconds)
	loadEnvInt(&c.WorkerCount, EnvWorkerCount)
	loadEnvInt(&c.WorkerBuffer, EnvWorkerBuffer)
	loadEnvInt(&c.MetricBuffer, EnvMetricBuffer)
	loadEnvInt(&c.MaxErrors, EnvMaxErrorCount)

	if tags := os.Getenv(EnvTags); tags != "" {
		c.Tags = strings.Split(tags, ",")
	}

	return c
}

// WithHost sets the host name for ddstats
func (c *Config) WithHost(Host string) *Config {
	c.Host = Host
	return c
}

// WithAPIKey set the api key. Instructions on how to acquire an api key
// can be found here https://docs.datadoghq.com/account_management/api-app-keys/
// If a client has been set with WithClient, then api key will be ignored.
func (c *Config) WithAPIKey(key string) *Config {
	c.APIKey = key
	return c
}

// WithNamespace set the namespace for ddstats
func (c *Config) WithNamespace(Namespace string) *Config {
	c.Namespace = Namespace
	return c
}

// WithTags set global slice of tags
func (c *Config) WithTags(tags []string) *Config {
	c.Tags = tags
	return c
}

// WithClient set the api client to use. If api key and client have both been set,
// the client will be used. If no client has been set, then a client will be created
// with the api key.
func (c *Config) WithClient(client client.APIClient) *Config {
	c.client = client
	return c
}

func loadEnvString(s *string, key string) {
	val := os.Getenv(key)
	if val != "" {
		*s = val
	}
}

func loadEnvInt(i *int, key string) {
	val := os.Getenv(key)
	if val == "" {
		return
	}

	v, _ := strconv.Atoi(val)
	*i = v
}

func loadEnvFloat64(f *float64, key string) {
	val := os.Getenv(key)
	if val == "" {
		return
	}

	v, _ := strconv.ParseFloat(val, 64)
	*f = v
}
