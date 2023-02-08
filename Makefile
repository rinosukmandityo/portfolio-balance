MIN_COVERAGE = 60
test:
	go test ./... -v -race -count=1 -cover -coverprofile=coverage.txt && go tool cover -func=coverage.txt \
	| grep total | tee /dev/stderr | sed 's/\%//g' \
	| awk '{err=0;c+=$$3}{if (c > 0 && c < $(MIN_COVERAGE)) {printf "=== FAIL: Coverage failed at %.2f%%\n", c; err=1}} END {exit err}'
	go test -bench=. -run=- ./...

generate:
	go generate ./...

lint:
	golangci-lint run --deadline=5m -v

gosec:
	gosec -exclude=G104 -fmt=json -exclude-dir=.go ./...

lint_docker:
	docker run --rm -v $(GOPATH)/pkg/mod:/go/pkg/mod:ro -v `pwd`:/`pwd`:ro -w /`pwd` golangci/golangci-lint:v1.39.0-alpine golangci-lint run --deadline=5m -v

# Cgo DNS resolver is used by default because of the issue with Kubernetes
# If you need to use pure Go DNS resolver you can remove `--tags netcgo` from the build
#
# Application must be statically-linked to run in `FROM scratch` container
build:
	go build --ldflags "-s -w -linkmode external -extldflags -static -X main.version=$(CI_BRANCH)" --tags netcgo -o ./bin/data-point-consumer ./cmd/data-point-consumer/
	go build --ldflags "-s -w -linkmode external -extldflags -static -X main.version=$(CI_BRANCH)" --tags netcgo -o ./bin/pending-kafka-consumer ./cmd/pending-kafka-consumer

up:
	docker-compose -f docker/docker-compose.yml up -d --build

down:
	docker-compose -f docker/docker-compose.yml down
