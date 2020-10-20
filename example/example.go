package main

import (
	"github.com/jmizell/DDStats"
	"log"
)

func main() {

	// Initialize the client with your namespace, host, api key, and your global tags
	stats := DDStats.NewStats("namespace", "host", "api_key", []string{"custom_tag:true"})

	// We can add a new metric by calling any of the methods, Increment,
	// Decrement, Count or Gauge. Increment increases a count metric by one.
	stats.Increment("metric1", nil)

	// Custom tags will be combined with the global tags on send to the api.
	stats.Increment("metric2", []string{"tag:1"})

	// Decrement decreases a count metric by 1.
	stats.Decrement("metric1", nil)

	// Count allows you to add an arbitrary value to a count metric.
	stats.Count("metric1", 10, nil)

	// Adding tags metrics are unique by name, and tags. Metric1 with nil
	// tags, and metric1 with one custom tag, are stored as two separate values.
	stats.Count("metric1", 10, []string{"tag:1"})

	// Gauge creates a gauge metric. The last value applied to the metric before
	// flush to the api, is the value sent.
	stats.Gauge("metric3", 10, nil)

	// Signal shutdown, and block until complete
	stats.Close()

	// Get a list of errors returned by the api
	if errors := stats.Errors(); len(errors) > 0 {
		for i, err := range errors {
			log.Printf("%d: %s", i, err.Error())
		}
		log.Fatalf("errors were founc")
	}
}
