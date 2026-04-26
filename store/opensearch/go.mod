module github.com/hossainshakhawat/distributed-logging/store-opensearch

go 1.25.0

require (
	github.com/hossainshakhawat/distributed-logging/store-kafka v0.0.0
	github.com/opensearch-project/opensearch-go/v2 v2.3.0
)

require github.com/stretchr/testify v1.11.1 // indirect

replace github.com/hossainshakhawat/distributed-logging/store-kafka => ../kafka
