FROM golang:1.21.5-bullseye AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go build -v -o apo-receiver ./cmd

FROM debian:bullseye-slim AS runner
WORKDIR /app
COPY receiver-config.yml /app/
COPY sqlscript /app/sqlscript/
COPY --from=builder /build/apo-receiver /app/
CMD ["/app/apo-receiver", "--config=/app/receiver-config.yml"]