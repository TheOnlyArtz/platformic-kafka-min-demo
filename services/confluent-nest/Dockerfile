FROM debian:bookworm AS librdkafka-builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    liblz4-dev \
    libsasl2-dev \
    libssl-dev \
    libzstd-dev \
    pkg-config \
    python3 \
    zlib1g-dev \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /tmp/librdkafka
RUN git clone --depth 1 --branch v2.13.2 https://github.com/confluentinc/librdkafka.git . \
  && ./configure \
  && make -j"$(nproc)" \
  && make install

FROM node:22-bookworm AS builder
ENV CKJS_LINKING=dynamic
ENV BUILD_LIBRDKAFKA=0
ENV LD_LIBRARY_PATH=/usr/local/lib
WORKDIR /app
COPY --from=librdkafka-builder /usr/local /usr/local
COPY package.json ./
RUN npm install
COPY tsconfig.json ./
COPY src ./src
COPY config ./config
RUN npm run build

FROM node:22-bookworm-slim
WORKDIR /app
ENV NODE_ENV=production
ENV LD_LIBRARY_PATH=/usr/local/lib
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    liblz4-1 \
    libsasl2-2 \
    libssl3 \
    libzstd1 \
    zlib1g \
  && rm -rf /var/lib/apt/lists/*
COPY --from=librdkafka-builder /usr/local /usr/local
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/config ./config
COPY package.json ./
CMD ["node", "dist/main.js"]
