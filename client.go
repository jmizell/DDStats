package DDStats

type Client struct {}

func NewClient() *Client {
	return &Client{}
}

func (c *Client) SendSeries(DDMetricSeries) error {
	panic("implement me")
}

func (c *Client) SendServiceCheck(*DDServiceCheck) error {
	panic("implement me")
}

func (c *Client) SendEvent(*DDEvent) error {
	panic("implement me")
}
