module github.com/distributed-logging/store-opensearch

go 1.22

require (
	github.com/distributed-logging/shared v0.0.0
	github.com/opensearch-project/opensearch-go/v2 v2.3.0
)

require github.com/stretchr/testify v1.11.1 // indirect

replace github.com/distributed-logging/shared => ../../shared
