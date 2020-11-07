package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

const (
	datadogAPIURL = "https://api.datadoghq.com/api/v1"
	encodingJSON  = "application/json"
)

var endpointSeries = fmt.Sprintf("%s/series", datadogAPIURL)
var endpointCheck = fmt.Sprintf("%s/check_run", datadogAPIURL)
var endpointEvent = fmt.Sprintf("%s/events", datadogAPIURL)

type APIClient interface {
	SendSeries(*DDMetricSeries) error
	SendServiceCheck(*DDServiceCheck) error
	SendEvent(*DDEvent) error
	SetHTTPClient(HTTPClient)
}

type HTTPClient interface {
	Post(url, contentType string, body io.Reader) (resp *http.Response, err error)
}

type DDClient struct {
	apiKey string
	client HTTPClient
}

func NewDDClient(apiKey string) *DDClient {
	return &DDClient{
		apiKey: apiKey,
		client: &http.Client{},
	}
}

func (c *DDClient) SetHTTPClient(client HTTPClient) {
	c.client = client
}

func (c *DDClient) SendSeries(series *DDMetricSeries) error {
	return c.post(series, encodingJSON, endpointSeries)
}

func (c *DDClient) SendServiceCheck(check *DDServiceCheck) error {
	return c.post(check, encodingJSON, endpointCheck)
}

func (c *DDClient) SendEvent(event *DDEvent) error {
	return c.post(event, encodingJSON, endpointEvent)
}

func (c *DDClient) post(payload interface{}, encoding, url string) error {

	// TODO implement retry logic

	url = fmt.Sprintf("%s?api_key=%s", url, c.apiKey)

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("could not marshal data to json, %s", err.Error())
	}

	response, err := c.client.Post(url, encoding, bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()

	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("could not read api response, %s", err.Error())
	}

	apiResponse := &DDApiResponse{}
	if err := json.Unmarshal(responseBytes, apiResponse); err != nil {
		return fmt.Errorf("could not read api response, %s", err.Error())
	}

	if response.StatusCode > 299 {
		return fmt.Errorf("api response %d: %s", response.StatusCode, strings.Join(apiResponse.Errors, ", "))
	}

	return nil
}

type DDApiResponse struct {
	Errors []string `json:"errors"`
}
