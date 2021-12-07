FROM ekidd/rust-musl-builder as builder

COPY --chown=rust:rust . .
RUN cargo build --target x86_64-unknown-linux-musl --release

FROM scratch
COPY ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /home/rust/src/target/x86_64-unknown-linux-musl/release/s3provisioner /usr/local/bin/s3provisioner
CMD ["s3provisioner"]