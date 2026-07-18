FROM golang:1.26-alpine AS build
WORKDIR /src/controlplane
COPY controlplane/go.mod controlplane/go.sum ./
RUN go mod download
COPY controlplane .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/model-control-plane ./cmd/model-control-plane

FROM alpine/k8s:1.34.0
COPY --from=build /out/model-control-plane /usr/local/bin/model-control-plane
ENTRYPOINT ["model-control-plane"]
