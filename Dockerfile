# UI build (Vite + React, embedded into the binary afterwards)
FROM oven/bun:1 AS ui
WORKDIR /ui
COPY ui/package.json ./
RUN bun install
COPY ui .
RUN bun run build

# rust:alpine targets musl, so the release binary is fully static.
FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY --from=ui /ui/dist ./ui/dist
RUN cargo build --release

FROM scratch
COPY --from=build /src/target/release/breezy-registry /breezy-registry
ENV BREEZY_LISTEN=0.0.0.0:5100 \
    BREEZY_DATA_DIR=/data
USER 65532:65532
EXPOSE 5100
VOLUME /data
ENTRYPOINT ["/breezy-registry"]
