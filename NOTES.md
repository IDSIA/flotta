_These notes will be aggregated in the README.md file once the dust of refactoring and debugging has settled._

# Execution modes

## Standalone

- 1 server
    - with in-memory database
- 1 client
    - with local configuration
- 3 local workers:
    - 1 for training
    - 1 for estimating
    - 1 for aggregating

## Server as aggregator (docker)

- 1 server
    - connected to database
- 1 PostgreSQL database
- 1 rabbitmq as message broker
- 1 celery worker (maybe more?) for aggregating
    - can perform all 3 tasks
    - will run only aggregations
- 1 redis for caching results
- (optional) 1 repository
    - used for updates distribution

## Client (docker)

- 1 client
    - with local configuration 
    - with 2 local workers


## Client (local)

- 1 client


## Server as full node (docker)

Same as aggregator, celery can perform client's tasks.
