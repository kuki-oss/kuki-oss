FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p kuki-cli \
 && install -D target/release/kuki /out/kuki

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /out/kuki /usr/local/bin/kuki
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/kuki"]
CMD ["serve"]
