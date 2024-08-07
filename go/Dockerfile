# syntax=docker.io/docker/dockerfile:1
FROM ubuntu:22.04 as build-stage

ARG DEBIAN_FRONTEND=noninteractive
RUN <<EOF
set -e
apt update
apt install -y --no-install-recommends \
    build-essential=12.9ubuntu3 \
    ca-certificates \
    g++-riscv64-linux-gnu=4:11.2.0--1ubuntu1 \
    wget
EOF

ARG GOVERSION=1.20.5

WORKDIR /opt/build

RUN wget https://go.dev/dl/go${GOVERSION}.linux-$(dpkg --print-architecture).tar.gz && \
  tar -C /usr/local -xzf go${GOVERSION}.linux-$(dpkg --print-architecture).tar.gz

ENV GOOS=linux
ENV GOARCH=riscv64
ENV CGO_ENABLED=1
ENV CC=riscv64-linux-gnu-gcc
ENV PATH=/usr/local/go/bin:${PATH}

COPY src .

RUN make

# runtime stage: produces final image that will be executed
FROM --platform=linux/riscv64 riscv64/ubuntu:22.04

LABEL io.cartesi.rollups.sdk_version=devel
LABEL io.cartesi.rollups.ram_size=128Mi
LABEL io.cartesi.rollups.sdk_name=zippiehq/cartesi-lambada-sdk

ARG DEBIAN_FRONTEND=noninteractive
RUN <<EOF
set -e
apt-get update
apt-get install -y --no-install-recommends \
  busybox-static=1:1.30.1-7ubuntu3
rm -rf /var/lib/apt/lists/* /var/log/* /var/cache/*
useradd --create-home --user-group dapp
EOF

ARG MACHINE_EMULATOR_TOOLS_VERSION=0.15.7
ADD https://github.com/zippiehq/cartesi-lambada-guest-tools/releases/download/v${MACHINE_EMULATOR_TOOLS_VERSION}/machine-emulator-tools-v${MACHINE_EMULATOR_TOOLS_VERSION}.deb /
RUN dpkg -i /machine-emulator-tools-v${MACHINE_EMULATOR_TOOLS_VERSION}.deb \
  && rm /machine-emulator-tools-v${MACHINE_EMULATOR_TOOLS_VERSION}.deb

ENV PATH="/opt/cartesi/bin:${PATH}"

WORKDIR /opt/cartesi/dapp
COPY --from=build-stage /opt/build/dapp .

ENV ROLLUP_HTTP_SERVER_URL="http://127.0.0.1:5004"
ENV LAMBADA_HTTP_SERVER_URL="http://127.0.0.1:5005"

ENTRYPOINT ["rollup-init"]
CMD ["lambada-http-server", "lambada-init", "/opt/cartesi/dapp/dapp"]
