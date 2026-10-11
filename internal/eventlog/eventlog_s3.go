package eventlog

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func parseURI(uri string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return "", "", fmt.Errorf("eventlog: store uri %q must be s3://<bucket>/<prefix>", uri)
	}
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("eventlog: store uri %q lacks a bucket", uri)
	}
	// Normalize a trailing slash so s3://bucket and s3://bucket/ derive the
	// same base key (issue #232).
	prefix = strings.Trim(prefix, "/")
	return bucket, prefix, nil
}

// newS3Client builds the S3 client both the writer and the reader use.
// Credentials follow the standard AWS chain unless an explicit key pair is
// given; Endpoint overrides the API target for MinIO-style stores and implies
// path-style addressing.
func newS3Client(ctx context.Context, region, endpoint, accessKey, secretKey string) (*s3.Client, error) {
	awsCfg, err := loadAWSConfig(ctx, region, endpoint)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if accessKey != "" && secretKey != "" {
			o.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		}
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	}), nil
}

func loadAWSConfig(ctx context.Context, region, endpoint string) (aws.Config, error) {
	if region == "" {
		region = "us-east-1"
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if endpoint != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(endpoint))
	}
	return awsconfig.LoadDefaultConfig(ctx, opts...)
}

// s3Putter adapts the S3 client to the putter interface.
type s3Putter struct {
	client *s3.Client
}

func (p *s3Putter) Put(ctx context.Context, bucket, key string, body []byte) error {
	_, err := p.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/x-ndjson"),
	})
	return err
}
