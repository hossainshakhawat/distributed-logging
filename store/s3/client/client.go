package client

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/distributed-logging/shared/models"
)

// Config holds S3 / compatible object-store settings.
type Config struct {
	Bucket   string
	Region   string
	Endpoint string // optional: set to a MinIO / LocalStack URL for local dev
}

// Client wraps the AWS S3 SDK for log-archive operations.
type Client struct {
	s3     *s3.Client
	bucket string
}

// New creates a Client using the default AWS credential chain.
// Set cfg.Endpoint to point at a local MinIO or LocalStack instance.
func New(ctx context.Context, cfg Config) (*Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, fmt.Errorf("s3 config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true // required for MinIO / LocalStack
		})
	}

	return &Client{
		s3:     s3.NewFromConfig(awsCfg, s3Opts...),
		bucket: cfg.Bucket,
	}, nil
}

// ArchivePath returns the canonical S3 key for a batch.
// Pattern: {tenant_id}/{yyyy}/{mm}/{dd}/{hh}/part-{part:05d}.gz
func ArchivePath(tenantID string, t time.Time, part int) string {
	return fmt.Sprintf("%s/%04d/%02d/%02d/%02d/part-%05d.gz",
		tenantID, t.Year(), int(t.Month()), t.Day(), t.Hour(), part)
}

// Upload gzip-compresses entries as newline-delimited JSON and stores them at key.
func (c *Client) Upload(ctx context.Context, key string, entries []models.LogEntry) error {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return fmt.Errorf("s3 encode: %w", err)
		}
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("s3 gzip close: %w", err)
	}

	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(c.bucket),
		Key:             aws.String(key),
		Body:            bytes.NewReader(buf.Bytes()),
		ContentEncoding: aws.String("gzip"),
		ContentType:     aws.String("application/x-ndjson"),
	})
	if err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

// Download retrieves and decompresses an archived object, returning decoded entries.
func (c *Client) Download(ctx context.Context, key string) ([]models.LogEntry, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer out.Body.Close()

	gz, err := gzip.NewReader(out.Body)
	if err != nil {
		return nil, fmt.Errorf("s3 gzip reader: %w", err)
	}
	defer gz.Close()

	var entries []models.LogEntry
	dec := json.NewDecoder(io.LimitReader(gz, 256<<20)) // 256 MB safety cap
	for dec.More() {
		var e models.LogEntry
		if err := dec.Decode(&e); err != nil {
			return nil, fmt.Errorf("s3 decode: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, nil
}
