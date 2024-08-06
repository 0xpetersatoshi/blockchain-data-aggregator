# Blockchain Data Aggregator for Marketplace Analytics

## Project Overview

This project contains a simple data pipeline that reads raw [sequence.xyz](https://sequence.xyz/) blockchain gaming transaction data, normalizes it, converts cryptocurrency values to USD, and writes it to a Clickhouse data warehouse.

### Key Components

- [cmd](./cmd/): Contains the main entrypoint script as well as a script that uses the coingecko api to generate a historical exchange rate dataset based on the sample data.
- [data](./data/): Contains the sample data that was used to generate the exchange rate dataset. These data sets have also been uploaded to a public GCP bucket.
  - The datasets are also available for download from the GCP Bucket:
    - [sample_data.csv](https://storage.googleapis.com/sequence-blockchain-data-aggregator/aggregator-data/sample_data.csv)
    - [exchange_rates.json](https://storage.googleapis.com/sequence-blockchain-data-aggregator/coingecko-data/exchange_rates.json)
- [internal](./internal/): Contains the core logic of the application, including API clients, configuration, data loaders, processors, and utilities.
- [scripts](./scripts/): Contains helper scripts for tasks such as uploading files to GCP.
- [init.sql](./init.sql): Contains the Clickhouse sql script to create the `transactions` table when the Clickhouse container is started.

## Dependencies

The dependencies to run this project are:

Required:

- [`go`](https://go.dev/dl/) version `1.21` or higher
- [`docker`](https://docs.docker.com/get-docker/) version 20.10 or higher

Optional:

- [`gcloud`](https://cloud.google.com/sdk/docs/install)

## Usage

To get started, run:

```bash
docker compose up -d

# or if using an older docker version
docker-compose up -d
```

This will start the clickhouse-server required for the data pipeline to write to.

In another terminal, run:

```bash
docker exec -it clickhouse clickhouse-client
```

This will provide you with a CLI client to the clickhouse instance where you can execute sql commands.

Finally, to run the data pipeline, run:

```bash
go run cmd/transactions/main.go
```

This will do the following:

- Read the sample data from the public GCP bucket
- Read the sample exchange rate data from the public GCP bucket (which was generated using [this script](./cmd/coingecko/generate-historical-exchange-rates-data/main.go))
- Normalize the data and convert prices to USD using the exchange rate data
- Write the data to the `transactions` table clickhouse data warehouse

You can provide the following CLI flags:

```bash
Usage of /var/folders/ft/k09lqv253znb92kr_450_6l00000gn/T/go-build2933272138/b001/exe/main:
      --db-conn-string string               database connection string (default "clickhouse://default:password@localhost:9000/default")
      --debug                               enable debug logging
      --exchange-rates-bucket-name string   source bucket name (default "sequence-blockchain-data-aggregator")
      --exchange-rates-object-path string   source object path (default "coingecko-data/exchange_rates.json")
      --source-bucket-name string           source bucket name (default "sequence-blockchain-data-aggregator")
      --source-object-path string           source object path (default "aggregator-data/sample_data.csv")
```

You shouldn't need to specify any flags as the defaults are set correctly.

Lastly, once the pipeline has finished running, you can run the following sql query in the clickhouse client instance:

```sql
select * from transactions final;
```

> Note: The `final` keyword is added to dedupe any results and make the result idempotent in case the pipeline is run multiple times.

## Cleanup

To clean up the docker containers, run:

```bash
docker compose down -v
```

This will stop the clickhouse-server and remove all volumes.
