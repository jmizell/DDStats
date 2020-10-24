package ddstats

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jmizell/ddstats/client"
)

const (
	DefaultWorkerCount   = 10
	DefaultWorkerBuffer  = 100
	DefaultFlushInterval = time.Second * 60
	DefaultMaxErrorCount = 100
	DefaultNamespace     = "ddstats"
)

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

type Config struct {
	Namespace            string   `json:"namespace"`
	Host                 string   `json:"host"`
	Tags                 []string `json:"tags"`
	APIKey               string   `json:"api_key"`
	FlushIntervalSeconds float64  `json:"flush_interval"`
	WorkerCount          int      `json:"worker_count"`
	WorkerBuffer         int      `json:"worker_buffer"`
	MetricBuffer         int      `json:"metric_buffer"`
	MaxErrors            int      `json:"max_errors"`

	client client.APIClient
}

func NewConfig() *Config {
	return &Config{
		Namespace:            DefaultNamespace,
		Tags:                 []string{},
		FlushIntervalSeconds: DefaultFlushInterval.Seconds(),
		WorkerCount:          DefaultWorkerCount,
		WorkerBuffer:         DefaultWorkerBuffer,
		MetricBuffer:         DefaultWorkerBuffer * DefaultWorkerCount,
		MaxErrors:            DefaultMaxErrorCount,
	}
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

func (c *Config) WithHost(Host string) *Config {
	c.Host = Host
	return c
}

func (c *Config) WithAPIKey(key string) *Config {
	c.APIKey = key
	return c
}

func (c *Config) WithNamespace(Namespace string) *Config {
	c.Namespace = Namespace
	return c
}

func (c *Config) WithTags(tags []string) *Config {
	c.Tags = tags
	return c
}

func (c *Config) WithClient(client client.APIClient) *Config {
	c.client = client
	return c
}
