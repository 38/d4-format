FROM rust:1.50 as builder
WORKDIR /usr/src/d4format
COPY . .

RUN cargo build --release

FROM debian:buster-slim
RUN rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/d4format/target/release/d4tools /usr/local/bin/d4tools

CMD ["d4tools"]
