FROM golang:1.27.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/vessel ./cmd/vessel

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -S vessel && adduser -S -G vessel vessel
COPY --from=build /out/vessel /usr/local/bin/vessel
USER vessel
EXPOSE 8080 9090
ENTRYPOINT ["vessel"]
CMD ["-http=0.0.0.0:8080", "-grpc=0.0.0.0:9090"]
