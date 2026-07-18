FROM rust:1.94-bookworm AS build
WORKDIR /app
COPY . .
RUN cargo build --release -p generation-worker

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/generation-worker /usr/local/bin/generation-worker
CMD ["generation-worker"]
