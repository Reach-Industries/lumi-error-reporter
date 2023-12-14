package lumiErrorReporter

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

type Reporter interface {
	OhDearWhatHappened(message string, code string, severity string, additionalInfo string)
	Close()
}

type reporter struct {
	MSK_BROKERS []string
	ERROR_TOPIC string
	OhDear      *kafka.Writer
	Source      string
}

type errorStructure struct {
	Source         string `json:"source"`
	Message        string `json:"message"`
	Code           string `json:"code"`
	Severity       string `json:"severity"`
	AdditionalInfo string `json:"additionalInfo"`
	Stacktrace     string `json:"stacktrace"`
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// Sends error messages to ohDear topic
func (r reporter) OhDearWhatHappened(message string, code string, severity string, additionalInfo string) {
	es := errorStructure{
		Source:         r.Source,
		Message:        message,
		Code:           code,
		Severity:       severity,
		AdditionalInfo: additionalInfo,
	}

	// Record stacktrace, ignoring this frame
	err := errors.New(message)
	stack := []string{}
	if err, ok := err.(stackTracer); ok {
		for i, f := range err.StackTrace() {
			if i != 0 {
				stack = append(stack, fmt.Sprintf("%+v", f))
			}
			// Only record the first 30 frames of the stacktrace
			if i > 30 {
				break
			}
		}
	}
	es.Stacktrace = strings.Join(stack, "\n")
	messageJson, _ := json.Marshal(es)

	r.OhDear.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(uuid.New().String()),
			Value: messageJson,
		})
}

func (r reporter) Close() {
	r.OhDear.Close()
}

func CreateLumiErrorReporter(source string, brokers []string, errorTopic string, isLocal bool) (reporterToReturn Reporter, err error) {

	err = validateReporterRequest(source, brokers, errorTopic)
	if err != nil {
		return nil, err
	}

	ohDear := getKafkaWriter(brokers, errorTopic, isLocal)

	reporterToReturn = reporter{
		MSK_BROKERS: brokers,
		ERROR_TOPIC: errorTopic,
		OhDear:      ohDear,
		Source:      source,
	}
	return
}

func getKafkaWriter(brokers []string, topic string, isLocal bool) *kafka.Writer {

	if isLocal {
		return &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		}
	}

	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		Transport: &kafka.Transport{
			TLS: &tls.Config{},
		},
	}
}

func validateReporterRequest(source string, brokers []string, errorTopic string) error {
	if source == "" {
		return errors.New("no error source provided")
	}

	if len(brokers) == 0 {
		return errors.New("MSK broker list empty")
	}

	if errorTopic == "" {
		return errors.New("error topic name not provided")
	}

	return nil
}
