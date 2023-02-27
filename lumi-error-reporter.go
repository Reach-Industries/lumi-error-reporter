package lumiErrorReporter

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	kafka "github.com/segmentio/kafka-go"
)

type Reporter interface {
	OhDearWhatHappened(thisHappened whatHappened)
	Close()
}

type reporter struct {
	MSK_BROKERS []string
	ERROR_TOPIC string
	OhDear      *kafka.Writer
	Source      string
}

type whatHappened struct {
	Message        string `json:"message"`
	Code           string `json:"code"`
	Severity       string `json:"severity"`
	AdditionalInfo string `json:"additionalInfo"`
}

type errorStructure struct {
	Source         string `json:"source"`
	Message        string `json:"message"`
	Code           string `json:"code"`
	Severity       string `json:"severity"`
	AdditionalInfo string `json:"additionalInfo"`
}

//Sends error messages to ohDear topic
func (r reporter) OhDearWhatHappened(thisHappened whatHappened) {
	es := errorStructure{
		Source:         r.Source,
		Message:        thisHappened.Message,
		Code:           thisHappened.Code,
		Severity:       thisHappened.Severity,
		AdditionalInfo: thisHappened.AdditionalInfo,
	}
	message, _ := json.Marshal(es)
	r.OhDear.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(uuid.New().String()),
			Value: message,
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
