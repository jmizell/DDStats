package DDStats

type Status int

// DDServiceCheck Status values
const (
	Okay = iota
	Warning
	Critical
	Unknown
)

type DDServiceCheck struct {
	Check     string   `json:"check"`
	Hostname  string   `json:"host_name"`
	Message   string   `json:"message"`
	Status    Status   `json:"status"`
	Tags      []string `json:"tags"`
	Timestamp int64    `json:"timestamp"`
}
