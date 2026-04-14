# docker build -t clickhouse/pytest-test .
FROM ubuntu:latest

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        git \
        openssl \
        python3-pip \
        zstd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt
