###########
# BUILDER #
###########

FROM rust:latest AS builder

RUN cargo install --git https://github.com/38/d4-format.git d4tools --branch master

#########
# FINAL #
#########

FROM debian:bookworm-slim

# Import lib from builder
COPY --from=builder /usr/local/cargo/bin/d4tools /usr/local/bin/d4tools

CMD ["d4tools"]
