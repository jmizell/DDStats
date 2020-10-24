package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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

	t.Run("api error", func(tt *testing.T) {
		client := NewDDClient("testKey")
		httpClient := newTestHTTPClient(http.StatusForbidden, "forbidden", nil)
		client.SetHTTPClient(httpClient)

		if err := client.SendEvent(&testEvent); err == nil {
			tt.Fatalf("expected an error, have nil")
		} else if err.Error() != "forbidden" {
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

	t.Run("api error", func(tt *testing.T) {
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
