# Build stage: compile a static binary with the Kafka client baked in.
FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /events .

# Runtime stage: small image, non-root user, no shell needed.
FROM alpine:3.21

RUN adduser -D -u 10002 appuser

COPY --from=build /events /usr/local/bin/events

USER appuser
EXPOSE 8082

ENTRYPOINT ["/usr/local/bin/events"]
