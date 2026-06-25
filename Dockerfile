FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /build/demo-app ./cmd/demo

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /build/demo-app /app/demo-app
COPY --from=builder /build/demo/config.yaml /app/demo/config.yaml
ENV DEMO_CONFIG=/app/demo/config.yaml
EXPOSE 50051
CMD ["/app/demo-app"]
