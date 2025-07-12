help:
	@echo "Available targets:"
	@echo "  run-main       - Run main.py for ETL"
	@echo "  test           - Run tests"
	@echo "  setup          - Install dependencies, and run migration"
	@echo "  service-start  - Run docker compose up"
	@echo "  service-stop   - Run docker compose down"
	@echo "  service-restart- Restart Docker Compose"
	@echo "  generate-proto - Generate Protobuf for Server and Client"

run-main:
	python pipeline/etl/main.py run start-etl

service-start:
	docker-compose up -d

service-stop:
	docker-compose down

service-restart:
	docker-compose down
	docker-compose up -d

setup:
	pip install -r pipeline/requirements.txt
	alembic -c pipeline/alembic.ini upgrade head
	@cd processor && go mod tidy

test:
	@docker exec -it btj-redis redis-cli flushall
	@pytest pipeline/tests/ -v -s

generate-proto: # adjust path based on your go environment
	@GOPATH=$(HOME)/go GOBIN=$(HOME)/go/bin PATH=$(PATH):$(HOME)/go/bin \
	protoc --go_out=processor/protos --go_opt=paths=source_relative --go-grpc_out=processor/protos --go-grpc_opt=paths=source_relative transform.proto
	@echo "Server[Go] Protobuf Created"
	@python -m grpc_tools.protoc -I=. --python_out=pipeline/protos --pyi_out=pipeline/protos --grpc_python_out=pipeline/protos transform.proto
	@echo "Client[Python] Protobuf Created"
