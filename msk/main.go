package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	msk "github.com/aws/aws-sdk-go-v2/service/kafka"
	types "github.com/aws/aws-sdk-go-v2/service/kafka/types"
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
				Operation:           kafka.ACLOperationTypeAll,
				ResourceType:        kafka.ResourceTypeCluster,
				ResourcePatternType: kafka.PatternTypeLiteral,
				ResourceName:        "kafka-cluster",
				Host:                "*",
			},
			{
				Principal:           "User:admin",
				PermissionType:      kafka.ACLPermissionTypeAllow,
				Operation:           kafka.ACLOperationTypeAll,
				ResourceType:        kafka.ResourceTypeTopic,
				ResourcePatternType: kafka.PatternTypeLiteral,
				ResourceName:        "*",
				Host:                "*",
			},
			{
				Principal:           "User:admin",
				PermissionType:      kafka.ACLPermissionTypeAllow,
				Operation:           kafka.ACLOperationTypeAll,
				ResourceType:        kafka.ResourceTypeTopic,
				ResourcePatternType: kafka.PatternTypePrefixed,
				ResourceName:        "*",
				Host:                "*",
			},
			{
				Principal:           "User:admin",
				PermissionType:      kafka.ACLPermissionTypeAllow,
				Operation:           kafka.ACLOperationTypeAll,
				ResourceType:        kafka.ResourceTypeGroup,
				ResourcePatternType: kafka.PatternTypeLiteral,
				ResourceName:        "*",
				Host:                "*",
			},
			{
				Principal:           "User:admin",
				PermissionType:      kafka.ACLPermissionTypeAllow,
				Operation:           kafka.ACLOperationTypeAll,
				ResourceType:        kafka.ResourceTypeGroup,
				ResourcePatternType: kafka.PatternTypePrefixed,
				ResourceName:        "*",
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

	//========= Make cluster public ===========
	// TODO need to fetch the cluster version: K3DWYIK6Y9EEQB
	arn := "arn:aws:kafka:eu-west-1:748861314284:cluster/poc-region1/96167cca-e109-43c8-a983-1e779e93f993-9"
	curVersion := "K3DWYIK6Y9EEQB"
	pub := "SERVICE_PROVIDED_EIPS"
	status, err := svc.UpdateConnectivity(context.TODO(), &msk.UpdateConnectivityInput{
		ClusterArn:     &arn,
		CurrentVersion: &curVersion,
		ConnectivityInfo: &types.ConnectivityInfo{
			PublicAccess: &types.PublicAccess{
				Type: &pub,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(status)
}
