package DDStats

type AlertType string

// DDEvent AlertType values
const (
	AlertError   = AlertType("error")
	AlertWarning = AlertType("warning")
	AlertInfo    = AlertType("info")
	AlertSuccess = AlertType("success")
)

type Priority string

// DDEvent Priority values
const (
	PriorityNormal = Priority("normal")
	PriorityLow    = Priority("low")
)

type DDEvent struct {
	AggregationKey string    `json:"aggregation_key"`
	AlertType      AlertType `json:"alert_type"`
	DateHappened   int64     `json:"date_happened"`
	DeviceName     string    `json:"device_name"`
	Host           string    `json:"host"`
	Priority       Priority  `json:"priority"`
	RelatedEventID int64     `json:"related_event_id"`
	SourceTypeName string    `json:"source_type_name"`
	Tags           []string  `json:"tags"`
	Title          string    `json:"title"`
}
