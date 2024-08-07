# syntax=docker.io/docker/dockerfile:1
FROM --platform=linux/riscv64 riscv64/ubuntu:22.04 as base

RUN apt-get update

FROM base as builder

ARG DEBIAN_FRONTEND=noninteractive
RUN <<EOF
set -e
apt-get install -y --no-install-recommends \
  build-essential=12.9ubuntu3 \
  ruby-dev="1:3.0~exp1" \
  ruby="1:3.0~exp1"
rm -rf /var/apt/lists/*
gem install bundler --no-document
EOF

COPY Gemfile Gemfile.lock ./

RUN <<EOF
set -e
bundle config set --without 'development test'
bundle install --jobs=3 --retry=3
EOF

FROM base

LABEL io.cartesi.rollups.sdk_version=devel
LABEL io.cartesi.rollups.ram_size=128Mi
LABEL io.cartesi.rollups.sdk_name=zippiehq/cartesi-lambada-sdk

ARG DEBIAN_FRONTEND=noninteractive
RUN <<EOF
set -e
apt-get update
apt-get install -y --no-install-recommends \
  busybox-static=1:1.30.1-7ubuntu3 \
  ruby="1:3.0~exp1"
rm -rf /var/lib/apt/lists/* /var/log/* /var/cache/*
useradd --create-home --user-group dapp
EOF

ARG MACHINE_EMULATOR_TOOLS_VERSION=0.15.7
ADD https://github.com/zippiehq/cartesi-lambada-guest-tools/releases/download/v${MACHINE_EMULATOR_TOOLS_VERSION}/machine-emulator-tools-v${MACHINE_EMULATOR_TOOLS_VERSION}.deb /
RUN dpkg -i /machine-emulator-tools-v${MACHINE_EMULATOR_TOOLS_VERSION}.deb \
  && rm /machine-emulator-tools-v${MACHINE_EMULATOR_TOOLS_VERSION}.deb

ENV PATH="/opt/cartesi/bin:${PATH}"

# Copy over gems from the dependencies stage
COPY --from=builder /var/lib/gems/ /var/lib/gems/

WORKDIR /usr/src/app
COPY . .

ENV ROLLUP_HTTP_SERVER_URL="http://127.0.0.1:5004"
ENV LAMBADA_HTTP_SERVER_URL="http://127.0.0.1:5005"

ENTRYPOINT ["rollup-init"]
CMD ["lambada-http-server", "lambada-init", "ruby", "main.rb"]
