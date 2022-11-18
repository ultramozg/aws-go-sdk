package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	msk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("eu-west-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	svc := msk.NewFromConfig(cfg)
	resp, err := svc.ListClustersV2(context.TODO(), &msk.ListClustersV2Input{})
	fmt.Println(resp)

	mechanism := aws_msk_iam_v2.NewMechanism(cfg)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"b-1-public.devspvdeveuwest1euwest.mv7mz5.c9.kafka.eu-west-1.amazonaws.com:9198", "b-2-public.devspvdeveuwest1euwest.mv7mz5.c9.kafka.eu-west-1.amazonaws.com:9198"},
		GroupID:     "some-consumer-group",
		GroupTopics: []string{"some-topic"},
		Dialer: &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			SASLMechanism: mechanism,
			TLS:           &tls.Config{},
		},
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatal(err)
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
