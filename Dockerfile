FROM rust:latest
WORKDIR /usr/src/d4format
COPY . .

RUN apt update
RUN yes | apt install git

RUN cargo install d4utils

CMD d4tools