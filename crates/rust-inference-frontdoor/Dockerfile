FROM rust:1.94-bookworm AS build
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler libprotobuf-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p inference-frontend

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/target/release/inference-frontend /usr/local/bin/inference-frontend
COPY --from=build /app/config/local.yaml /app/config/service.yaml
EXPOSE 8080
CMD ["inference-frontend"]
