# BTJ Academy ETL
An example full etl project using Python and Go

## Prerequisites

Before running your local machine, you need to install the required tools:

1. Python
2. Go
3. [Air](https://github.com/air-verse/air) (optional if want directly run processor app in local)
4. Docker Runtime

## Setup

- Make your python virtual environment and activate
- Run `make service-start`, or basically `docker-compose up -d`
- Processor app also deploy as container, check docker ps (but still can hot reload if something change on files on processor/)
- If you want to run air on local, just comment processor service on `docker-compose.yml`
- Run `make setup`
- If there is no error, check `docker logs -f btj-processor` wait if still building until the server is listening, then run `make test` to make sure everything is setup properly

## Run Local

- This need 3 window terminal to run all etl pipeline
- On first window, run
  ```
  celery --app pipeline.worker_app worker --concurrency 1 --pool solo --queues etl --loglevel=INFO
  ```
- On second window, run
  ```
  make run-main
  ```
- Last, on third window, for check logging on processor app, check logs container or air app


## Additional

If you want to add new contract protobuf on transform.proto

After editing transform.proto file, run `make generate-proto`

Then edit the generated file on `pipeline/protos` that might cause the code error
