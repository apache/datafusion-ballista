# Minimal Riffle test container with Uniffle coordinator + Riffle server.
# Used by testcontainers in integration tests.
#
# Build: docker build -t riffle-test -f ballista/riffle/docker/Dockerfile .
# (from the riffle repo root)
#
# Run as coordinator: docker run -p 19995:19995 -p 21000:21000 riffle-test coordinator
# Run as riffle server: docker run -p 19998:19998 -p 21100:21100 riffle-test server

FROM eclipse-temurin:17-jre AS coordinator-base

# Download Uniffle coordinator
ARG UNIFFLE_VERSION=0.10.0
RUN mkdir -p /opt/uniffle && \
    apt-get update && apt-get install -y curl && \
    curl -sL https://archive.apache.org/dist/uniffle/${UNIFFLE_VERSION}/apache-uniffle-${UNIFFLE_VERSION}-bin.tar.gz | \
    tar xz -C /opt/uniffle --strip-components=1

# Configure coordinator
RUN mkdir -p /opt/uniffle/conf /opt/uniffle/logs
COPY coordinator.conf /opt/uniffle/conf/coordinator.conf

FROM rust:1-slim AS riffle-builder
# This stage is only needed if building riffle from source.
# For prebuilt binaries, skip this stage and COPY the binary directly.

FROM eclipse-temurin:17-jre

# Copy Uniffle coordinator
COPY --from=coordinator-base /opt/uniffle /opt/uniffle
ENV UNIFFLE_HOME=/opt/uniffle
ENV PATH="${UNIFFLE_HOME}/bin:${PATH}"

# Copy riffle server binary (must be built externally and placed in docker context)
# Build with: cd ~/git/riffle && cargo build --release -p riffle-server
# Then: cp target/release/riffle-server ballista/riffle/docker/
COPY riffle-server /opt/riffle/riffle-server
COPY riffle.toml /opt/riffle/riffle.toml

RUN chmod +x /opt/riffle/riffle-server && \
    mkdir -p /tmp/riffle-data /tmp/riffle-logs

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["coordinator"]
