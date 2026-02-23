package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/handlers"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"resty.dev/v3"
)

type collectorsResponse []string

type collectorStatusResponse struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type sourcePartition struct {
	Cluster   string `json:"cluster"`
	Partition int    `json:"partition"`
	Topic     string `json:"topic"`
}

type sourceOffset struct {
	Offset int `json:"offset"`
}

type sinkPartition struct {
	Partition int    `json:"kafka_partition"`
	Topic     string `json:"kafka_topic"`
}

type sinkOffset struct {
	Offset int `json:"kafka_offset"`
}

type sourceCollectorOffsetsResponse struct {
	Offsets []struct {
		Partition sourcePartition `json:"partition"`
		Offset    sourceOffset    `json:"offset"`
	} `json:"offsets"`
}

type sinkCollectorOffsetsResponse struct {
	Offsets []struct {
		Partition sinkPartition `json:"partition"`
		Offset    sinkOffset    `json:"offset"`
	} `json:"offsets"`
}

type restAPICollector struct {
	offset *prometheus.Desc

	api *resty.Client
}

func newRestAPICollector(api_url string) (prometheus.Collector, error) {

	api := resty.New().SetBaseURL(api_url)

	// Test the API connection
	response, err := api.R().Get("/connectors")
	if err != nil {
		return nil, fmt.Errorf("error connecting to Kafka Connect API at %s: %v", api_url, err)
	}
	if response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code from Kafka Connect API at %s: %d", api_url, response.StatusCode())
	}

	return &restAPICollector{
		offset: prometheus.NewDesc("kafka_connect_current_offset",
			"The current offset of the Kafka Connect connector",
			[]string{"connector", "cluster", "topic", "partition"}, nil,
		),
		api: api,
	}, nil
}

// Each and every collector must implement the Describe function.
// It essentially writes all descriptors to the prometheus desc channel.
func (collector *restAPICollector) Describe(ch chan<- *prometheus.Desc) {

	//Update this section with the each metric you create for a given collector
	ch <- collector.offset
}

// Collect implements required collect function for all promehteus collectors
func (collector *restAPICollector) Collect(ch chan<- prometheus.Metric) {

	response, err := collector.api.R().SetResult(&collectorsResponse{}).Get("/connectors")
	if err != nil {
		log.Printf("Error fetching connectors: %v", err)
		return
	}
	for _, connector := range *response.Result().(*collectorsResponse) {

		response, err := collector.api.R().SetResult(&collectorStatusResponse{}).Get(fmt.Sprintf("/connectors/%s/status", connector))
		if err != nil {
			log.Printf("Error fetching status for connector %s: %v", connector, err)
			continue
		}
		connectorStatus := response.Result().(*collectorStatusResponse)

		if connectorStatus.Type == "source" {
			collector.collectSourceOffsets(ch, connector)
		} else if connectorStatus.Type == "sink" {
			collector.collectSinkOffsets(ch, connector)
		} else {
			log.Printf("Unknown connector type for connector %s: %s", connector, connectorStatus.Type)
		}
	}
}

func (collector *restAPICollector) collectSourceOffsets(ch chan<- prometheus.Metric, connector string) {
	response, err := collector.api.R().SetResult(&sourceCollectorOffsetsResponse{}).Get(fmt.Sprintf("/connectors/%s/offsets", connector))

	if err != nil {
		log.Printf("Error fetching offsets for connector %s: %v", connector, err)
		return
	}
	for _, offset := range response.Result().(*sourceCollectorOffsetsResponse).Offsets {
		ch <- prometheus.MustNewConstMetric(
			collector.offset,
			prometheus.GaugeValue,
			float64(offset.Offset.Offset),
			connector,
			offset.Partition.Cluster,
			offset.Partition.Topic,
			fmt.Sprintf("%d", offset.Partition.Partition),
		)
	}
}

func (collector *restAPICollector) collectSinkOffsets(ch chan<- prometheus.Metric, connector string) {
	response, err := collector.api.R().SetResult(&sinkCollectorOffsetsResponse{}).Get(fmt.Sprintf("/connectors/%s/offsets", connector))

	if err != nil {
		log.Printf("Error fetching offsets for connector %s: %v", connector, err)
		return
	}
	for _, offset := range response.Result().(*sinkCollectorOffsetsResponse).Offsets {
		ch <- prometheus.MustNewConstMetric(
			collector.offset,
			prometheus.GaugeValue,
			float64(offset.Offset.Offset),
			connector,
			"",
			offset.Partition.Topic,
			fmt.Sprintf("%d", offset.Partition.Partition),
		)
	}
}

func main() {
	connectAPI, port, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	collector, err := newRestAPICollector(connectAPI)
	if err != nil {
		log.Fatalf("Error creating REST API collector: %v", err)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collector,
	)

	http.Handle("/metrics", handlers.LoggingHandler(os.Stdout, promhttp.HandlerFor(reg, promhttp.HandlerOpts{})))
	log.Printf("Connect API at %v", connectAPI)
	log.Printf("Starting server on port %d", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func parseArgs(args []string) (string, int, error) {
	fs := flag.NewFlagSet("kafka-connect-rest-exporter", flag.ContinueOnError)
	// https://github.com/prometheus/prometheus/wiki/Default-port-allocations
	// says that 9840 is the default port for Kafka Connect exporter, so we will
	// use it as default here as well.
	port := fs.Int("port", 9840, "Port to bind the metrics server to")

	if err := fs.Parse(args); err != nil {
		return "", 0, err
	}

	remaining := fs.Args()
	if len(remaining) < 1 {
		return "", 0, fmt.Errorf("missing required argument: connect-api")
	}

	if len(remaining) > 1 {
		return "", 0, fmt.Errorf("unexpected extra arguments: %v", remaining[1:])
	}

	return remaining[0], *port, nil
}
