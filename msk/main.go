package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	msk "github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

func isMSKPublic(svc *msk.Client, arn *string) (bool, error) {
	data, err := svc.GetBootstrapBrokers(context.TODO(), &msk.GetBootstrapBrokersInput{
		ClusterArn: arn,
	})
	if err != nil {
		return false, err
	}
	if data.BootstrapBrokerStringPublicSaslIam != nil {
		return true, nil
	}
	return false, nil
}

func makeMSKPublic(svc *msk.Client, arn *string, public bool) error {
	if isPublic, err := isMSKPublic(svc, arn); (err == nil && isPublic && public) || (err == nil && !isPublic && !public) {
		log.Println("MSK not need to update")
		return nil
	}

	publicConnectivity := "DISABLED"
	if public {
		publicConnectivity = "SERVICE_PROVIDED_EIPS"
	}
	info, err := svc.DescribeCluster(context.TODO(), &msk.DescribeClusterInput{ClusterArn: arn})

	if err != nil {
		log.Fatal(err)
	}

	_, err = svc.UpdateConnectivity(context.TODO(), &msk.UpdateConnectivityInput{
		ClusterArn:     arn,
		CurrentVersion: info.ClusterInfo.CurrentVersion,
		ConnectivityInfo: &types.ConnectivityInfo{
			PublicAccess: &types.PublicAccess{
				Type: &publicConnectivity,
			},
		},
	})

	if err != nil {
		return err
	}

	// Wait unit the cluster will be public
	for {
		if flag, err := isMSKPublic(svc, arn); err == nil && flag {
			break
		}
		fmt.Println("MSK is updating")
		time.Sleep(60 * time.Second)
	}

	log.Println("MSK is public")
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
		Addr:      kafka.TCP(append(strings.Split(*brokers.BootstrapBrokerStringPublicSaslIam, ","), strings.Split(*brokers.BootstrapBrokerStringSaslIam, ",")...)...),
		Timeout:   30 * time.Second,
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
	arn := flag.String("arn", "", "Please provide arn of the MSK cluster")
	public := flag.Bool("public", false, "Make MSK public")
	acl := flag.Bool("acl", false, "Update acl for the user")
	flag.Parse()

	if *arn == "" {
		fmt.Println("Error: please provide the MSK cluster arn")
		os.Exit(1)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("eu-west-1"))
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	svc := msk.NewFromConfig(cfg)

	err = makeMSKPublic(svc, arn, *public)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	if *acl {
		err = updateACLs(cfg, svc, arn)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
	}
	os.Exit(0)
}
