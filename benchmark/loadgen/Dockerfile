FROM rust:alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY Cargo.toml ./
COPY Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM scratch
COPY --from=build /build/target/release/loadgen /loadgen
ENTRYPOINT ["/loadgen"]
CMD ["sleep"]
