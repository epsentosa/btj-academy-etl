# BTJ Academy ETL
An example full etl project using Python and Go

## Prerequisites

Before running your local machine, you need to install the required tools:

1. Python
2. Go
3. [Air](https://github.com/air-verse/air)
4. Docker Runtime

## Setup

- Make your python virtual environment and activate
- Run `make service-start`, or basically `docker-compose up -d`
- Run `make setup`
- If there is no error, you could try to run `air` then on other terminal window run `make test` to make sure everything is setup properly

## Run Local

- This need 3 window terminal to run all etl pipeline
- On first window, just run 
  ```
  air
  ```
- On second window, run
  ```
  celery --app pipeline.worker_app worker --concurrency 1 --pool solo --queues etl --loglevel=INFO
  ```
- Last, on third window, run
  ```
  make run-main
  ```


## Additional

If you want to add new contract protobuf on transform.proto

After editing transform.proto file, run `make generate-proto`

Then edit the generated file on `pipeline/protos` that might cause the code error
