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
	transport := &kafka.Transport{
		SASL: mechanism,
		TLS:  &tls.Config{},
	}
	client := &kafka.Client{
		Addr:      kafka.TCP("b-1-public.pocregion1.b07vs9.c9.kafka.eu-west-1.amazonaws.com:9198", "b-2-public.pocregion1.b07vs9.c9.kafka.eu-west-1.amazonaws.com:9198"),
		Timeout:   5 * time.Second,
		Transport: transport,
	}

	res, err := client.CreateACLs(context.Background(), &kafka.CreateACLsRequest{
		ACLs: []kafka.ACLEntry{
			{
				Principal:           "User:admin",
				PermissionType:      kafka.ACLPermissionTypeAllow,
				Operation:           kafka.ACLOperationTypeRead,
				ResourceType:        kafka.ResourceTypeTopic,
				ResourcePatternType: kafka.PatternTypeLiteral,
				ResourceName:        "fake-topic-for-alice",
				Host:                "*",
			},
			{
				Principal:           "User:admin",
				PermissionType:      kafka.ACLPermissionTypeAllow,
				Operation:           kafka.ACLOperationTypeRead,
				ResourceType:        kafka.ResourceTypeGroup,
				ResourcePatternType: kafka.PatternTypeLiteral,
				ResourceName:        "fake-group-for-bob",
				Host:                "*",
			},
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	for _, err := range res.Errors {
		if err != nil {
			log.Println(err)
		}
	}
}
