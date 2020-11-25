package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
	"time"
)

type testHTTPClient struct {
	callURL         string
	callContentType string
	callBody        []byte

	response *http.Response
	error    error
}

func newTestHTTPClient(status int, apiError string, err error) *testHTTPClient {

	apiResp := &DDApiResponse{}
	if apiError != "" {
		apiResp.Errors = []string{apiError}
	}
	data, _ := json.Marshal(apiResp)

	return &testHTTPClient{
		error: err,
		response: &http.Response{
			StatusCode: status,
			Body:       ioutil.NopCloser(bytes.NewReader(data)),
		},
	}
}

func (t *testHTTPClient) Post(url, contentType string, body io.Reader) (resp *http.Response, err error) {

	t.callURL = url
	t.callContentType = contentType
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, err
	}
	t.callBody = data

	return t.response, t.error
}

func TestDDClient_SendEvent(t *testing.T) {

	testEvent := DDEvent{
		AggregationKey: "testEvent",
		AlertType:      AlertError,
		DateHappened:   time.Now().Unix(),
		DeviceName:     "device1",
		Host:           "host1",
		Priority:       PriorityNormal,
		SourceTypeName: "",
		Tags:           nil,
		Title:          "",
	}

	callURL := "https://api.datadoghq.com/api/v1/events?api_key=testKey"

	t.Run("no error", func(tt *testing.T) {
		client := NewDDClient("testKey")
		httpClient := newTestHTTPClient(http.StatusOK, "", nil)
		client.SetHTTPClient(httpClient)

		if err := client.SendEvent(&testEvent); err != nil {
			tt.Fatalf("expected no error, have %s", err.Error())
		}

		if httpClient.callURL != callURL {
			tt.Fatalf("expected request url to be %s, have %s", callURL, httpClient.callURL)
		}

		if httpClient.callContentType != encodingJSON {
			tt.Fatalf("expected request contentType to be %s, have %s", encodingJSON, httpClient.callContentType)
		}

		if len(httpClient.callBody) == 0 {
			tt.Fatalf("expected request body to be present")
		}
	})

	t.Run("api error forbidden", func(tt *testing.T) {
		client := NewDDClient("testKey")
		httpClient := newTestHTTPClient(http.StatusForbidden, "forbidden", nil)
		client.SetHTTPClient(httpClient)

		if err := client.SendEvent(&testEvent); err == nil {
			tt.Fatalf("expected an error, have nil")
		} else if err.Error() != "api response 403: forbidden" {
			tt.Fatalf("expected error to be %s, have %s", "forbidden", err.Error())
		}

		if httpClient.callURL != callURL {
			tt.Fatalf("expected request url to be %s, have %s", callURL, httpClient.callURL)
		}

		if httpClient.callContentType != encodingJSON {
			tt.Fatalf("expected request contentType to be %s, have %s", encodingJSON, httpClient.callContentType)
		}

		if len(httpClient.callBody) == 0 {
			tt.Fatalf("expected request body to be present")
		}
	})

	t.Run("client error", func(tt *testing.T) {
		client := NewDDClient("testKey")
		httpClient := newTestHTTPClient(0, "", fmt.Errorf("client error"))
		client.SetHTTPClient(httpClient)

		if err := client.SendEvent(&testEvent); err == nil {
			tt.Fatalf("expected an error, have nil")
		} else if err.Error() != "client error" {
			tt.Fatalf("expected error to be %s, have %s", "client error", err.Error())
		}

		if httpClient.callURL != callURL {
			tt.Fatalf("expected request url to be %s, have %s", callURL, httpClient.callURL)
		}

		if httpClient.callContentType != encodingJSON {
			tt.Fatalf("expected request contentType to be %s, have %s", encodingJSON, httpClient.callContentType)
		}

		if len(httpClient.callBody) == 0 {
			tt.Fatalf("expected request body to be present")
		}
	})
}

func TestDDClient_SendSeries(t *testing.T) {

	testSeries := DDMetricSeries{
		Series: []*DDMetric{
			{
				Host:     "host1",
				Interval: 60,
				Metric:   "test.metric",
				Points: [][2]interface{}{{time.Now().Unix(), 1}},
				Tags:     []string{"tag:1"},
				Type:     Count,
			},
		},
	}

	callURL := "https://api.datadoghq.com/api/v1/series?api_key=testKey"

	t.Run("no error", func(tt *testing.T) {
		client := NewDDClient("testKey")
		httpClient := newTestHTTPClient(http.StatusOK, "", nil)
		client.SetHTTPClient(httpClient)

		if err := client.SendSeries(&testSeries); err != nil {
			tt.Fatalf("expected no error, have %s", err.Error())
		}

		if httpClient.callURL != callURL {
			tt.Fatalf("expected request url to be %s, have %s", callURL, httpClient.callURL)
		}

		if httpClient.callContentType != encodingJSON {
			tt.Fatalf("expected request contentType to be %s, have %s", encodingJSON, httpClient.callContentType)
		}

		if len(httpClient.callBody) == 0 {
			tt.Fatalf("expected request body to be present")
		}
	})
}


func TestDDClient_SendServiceCheck(t *testing.T) {

	testCheck := DDServiceCheck{
		Check:     "test",
		Hostname:  "testHost",
		Message:   "check ran",
		Status:    1,
		Tags:     []string{"tag:1"},
		Timestamp: time.Now().Unix(),
	}

	callURL := "https://api.datadoghq.com/api/v1/check_run?api_key=testKey"

	t.Run("no error", func(tt *testing.T) {
		client := NewDDClient("testKey")
		httpClient := newTestHTTPClient(http.StatusOK, "", nil)
		client.SetHTTPClient(httpClient)

		if err := client.SendServiceCheck(&testCheck); err != nil {
			tt.Fatalf("expected no error, have %s", err.Error())
		}

		if httpClient.callURL != callURL {
			tt.Fatalf("expected request url to be %s, have %s", callURL, httpClient.callURL)
		}

		if httpClient.callContentType != encodingJSON {
			tt.Fatalf("expected request contentType to be %s, have %s", encodingJSON, httpClient.callContentType)
		}

		if len(httpClient.callBody) == 0 {
			tt.Fatalf("expected request body to be present")
		}
	})
}

func TestDDClient_post(t *testing.T) {

	t.Run("nil payload", func(t *testing.T) {
		client := NewDDClient("testKey")
		httpClient := newTestHTTPClient(0, "", nil)
		client.SetHTTPClient(httpClient)

		if err := client.post(make(chan int), "", ""); err == nil {
			t.Fatalf("expected an error, have nil")
		} else if !strings.HasPrefix(err.Error(), "could not marshal data to json") {
			t.Fatalf("expected error to have prefix \"%s\", have \"%s\"", "could not marshal data to json", err.Error())
		}
	})

	t.Run("bad api response empty body", func(t *testing.T) {
		client := NewDDClient("testKey")
		httpClient := &testHTTPClient{
			error: nil,
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       ioutil.NopCloser(bytes.NewReader([]byte(""))),
			},
		}
		client.SetHTTPClient(httpClient)

		if err := client.post(nil, "", ""); err == nil {
			t.Fatalf("expected an error, have nil")
		} else if !strings.HasPrefix(err.Error(), "could not read api response") {
			t.Fatalf("expected error to have prefix \"%s\", have \"%s\"", "could not read api response", err.Error())
		}
	})
}