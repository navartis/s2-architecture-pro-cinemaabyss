# Build stage: compile a static binary.
FROM golang:1.26-alpine AS build

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /proxy .

# Runtime stage: small image, non-root user, no shell needed.
FROM alpine:3.21

RUN adduser -D -u 10001 appuser

COPY --from=build /proxy /usr/local/bin/proxy

USER appuser
EXPOSE 8000

ENTRYPOINT ["/usr/local/bin/proxy"]
