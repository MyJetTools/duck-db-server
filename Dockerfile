FROM ubuntu:22.04
COPY ./target/release/duck-db-server ./target/release/duck-db-server
ENTRYPOINT ["./target/release/duck-db-server"]