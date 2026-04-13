# ── Stage 1: Build C++ cache node ────────────────────────────────────────
FROM ubuntu:22.04 AS builder

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \
    build-essential cmake git pkg-config \
    libgrpc++-dev protobuf-compiler-grpc \
    libprotobuf-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY cpp/ .

# Generate protobuf/gRPC code and build
RUN mkdir -p build && cd build && \
    cmake .. -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=OFF && \
    make -j$(nproc) cache_node_server

# ── Stage 2: Minimal runtime image ───────────────────────────────────────
FROM ubuntu:22.04 AS runtime

RUN apt-get update && apt-get install -y \
    libgrpc++ libprotobuf23 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN useradd -r -s /bin/false mercury
WORKDIR /app
COPY --from=builder /src/build/cache_node_server .
RUN chown -R mercury:mercury /app
USER mercury

ENV MERCURY_ADDRESS=0.0.0.0:50051
ENV MERCURY_NODE_ID=node-0
ENV MERCURY_MAX_MB=256

EXPOSE 50051

ENTRYPOINT ["/app/cache_node_server"]
CMD ["0.0.0.0:50051", "node-0", "256"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD grpc_health_probe -addr=localhost:50051 || exit 1
