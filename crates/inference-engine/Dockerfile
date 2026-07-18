FROM rust:1.94-bookworm AS build
WORKDIR /app
COPY . .
RUN cargo build --release -p inference-engine --bin inference-engine

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/inference-engine /usr/local/bin/inference-engine
CMD ["inference-engine"]
