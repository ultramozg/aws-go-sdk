package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	msk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

func makeMSKPublic(svc *msk.Client, arn *string) error {
	pub := "SERVICE_PROVIDED_EIPS"
	info, err := svc.DescribeCluster(context.TODO(), &msk.DescribeClusterInput{ClusterArn: arn})

	if err != nil {
		log.Fatal(err)
	}

	status, err := svc.UpdateConnectivity(context.TODO(), &msk.UpdateConnectivityInput{
		ClusterArn:     arn,
		CurrentVersion: info.ClusterInfo.CurrentVersion,
		ConnectivityInfo: &types.ConnectivityInfo{
			PublicAccess: &types.PublicAccess{
				Type: &pub,
			},
		},
	})

	if err != nil {
		return err
	}
	log.Println(status)

	// Wait unit the cluster will be public
	for {
		data, err := svc.GetBootstrapBrokers(context.TODO(), &msk.GetBootstrapBrokersInput{
			ClusterArn: arn,
		})
		if err != nil {
			log.Fatal(err)
		}
		if data.BootstrapBrokerStringPublicSaslIam != nil {
			fmt.Println("INFO: ", *data.BootstrapBrokerStringPublicSaslIam)
			break
		}
		fmt.Println("MSK is updating")
		time.Sleep(60 * time.Second)
	}

	return nil
}

func updateACLs(cfg aws.Config, svc *msk.Client, arn *string) error {
	brokers, err := svc.GetBootstrapBrokers(context.TODO(), &msk.GetBootstrapBrokersInput{ClusterArn: arn})
	if err != nil {
		log.Fatal(err)
	}
	mechanism := aws_msk_iam_v2.NewMechanism(cfg)
	transport := &kafka.Transport{
		SASL: mechanism,
		TLS:  &tls.Config{},
	}
	client := &kafka.Client{
		Addr:      kafka.TCP(*brokers.BootstrapBrokerStringSaslIam),
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
		return err
	}

	for _, err := range res.Errors {
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("eu-west-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	svc := msk.NewFromConfig(cfg)

	arn := "sdfsfsdf"
	makeMSKPublic(svc, &arn)
	updateACLs(cfg, svc, &arn)
}
