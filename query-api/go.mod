module github.com/distributed-logging/query-api

go 1.22

require (
	github.com/distributed-logging/shared v0.0.0
	github.com/distributed-logging/store-opensearch v0.0.0
)

require github.com/opensearch-project/opensearch-go/v2 v2.3.0 // indirect

replace (
	github.com/distributed-logging/shared => ../shared
	github.com/distributed-logging/store-opensearch => ../store/opensearch
)
