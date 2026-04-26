module github.com/distributed-logging/stream-processor

go 1.25.0

require (
	github.com/distributed-logging/shared v0.0.0
	github.com/distributed-logging/store-kafka v0.0.0
	github.com/distributed-logging/store-opensearch v0.0.0
	github.com/distributed-logging/store-redis v0.0.0
	github.com/distributed-logging/store-s3 v0.0.0
)

require (
	github.com/aws/aws-sdk-go-v2 v1.41.6 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.9 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.32.16 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.15 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.14 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/s3 v1.100.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.20 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.42.0 // indirect
	github.com/aws/smithy-go v1.25.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/opensearch-project/opensearch-go/v2 v2.3.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/redis/go-redis/v9 v9.18.0 // indirect
	github.com/twmb/franz-go v1.21.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace (
	github.com/distributed-logging/shared => ../shared
	github.com/distributed-logging/store-kafka => ../store/kafka
	github.com/distributed-logging/store-opensearch => ../store/opensearch
	github.com/distributed-logging/store-redis => ../store/redis
	github.com/distributed-logging/store-s3 => ../store/s3
)
