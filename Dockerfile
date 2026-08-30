# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build

ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags='-s -w' -o /out/s3-lease-e2e ./test/e2e/candidate

FROM scratch
COPY --from=build /out/s3-lease-e2e /s3-lease-e2e
ENTRYPOINT ["/s3-lease-e2e"]
