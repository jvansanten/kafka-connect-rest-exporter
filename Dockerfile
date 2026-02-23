# Multi-stage build for minimal container image
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o kafka-connect-rest-exporter .

# Final stage with distroless image for minimal footprint
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

COPY --from=builder /app/kafka-connect-rest-exporter /kafka-connect-rest-exporter

# User is nonroot (UID 65532)
USER nonroot:nonroot

# Expose metrics port
EXPOSE 9840

ENTRYPOINT ["/kafka-connect-rest-exporter"]
