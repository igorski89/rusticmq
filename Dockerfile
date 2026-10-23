FROM rust:alpine AS build
RUN apk add --no-cache musl-dev ca-certificates binutils
WORKDIR /build
COPY Cargo.toml Cargo.lock build.rs ./
COPY vendor ./vendor
COPY src ./src
COPY LICENSE.txt NOTICE.txt ./
# The build context has no .git; pass the commit for rusticmq_build_info.
ARG RUSTICMQ_COMMIT=unknown
RUN cargo build --locked --profile container \
    && ! readelf --program-headers target/container/rusticmq | grep -q INTERP \
    && ! readelf --dynamic target/container/rusticmq | grep -q NEEDED \
    && mkdir -p /runtime/data /runtime/tmp \
    && chmod 1777 /runtime/tmp

FROM build AS test
COPY tests ./tests
RUN cargo test --locked --all-targets

FROM scratch
COPY --from=build /build/target/container/rusticmq /usr/local/bin/rusticmq
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build --chown=10001:10001 /runtime/data /data
COPY --from=build /runtime/tmp /tmp
COPY --from=build /build/LICENSE.txt /build/NOTICE.txt /usr/share/doc/rusticmq/
COPY --from=build /build/vendor/hocon/LICENSE /usr/share/doc/rusticmq/hocon/LICENSE
USER 10001:10001
WORKDIR /data
EXPOSE 9324
ENTRYPOINT ["/usr/local/bin/rusticmq"]
