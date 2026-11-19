# Development image. The source is bind-mounted over /src by docker-compose.yaml.
# Keep the Go version in sync with .github/workflows/ci.yml (GO_VERSION).
FROM golang:1.26

WORKDIR /src

# Warm the module cache in its own layer. go.sum does not exist until the first dependency.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN go build ./...

CMD ["go", "test", "./..."]
