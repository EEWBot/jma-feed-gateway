FROM rust:1.97.1-trixie AS build-env
LABEL maintainer="sjcl"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

WORKDIR /usr/src
COPY . /usr/src/jma-feed-gateway/
WORKDIR /usr/src/jma-feed-gateway
RUN cargo build --release && cargo install cargo-license && cargo license \
	--authors \
	--do-not-bundle \
	--avoid-dev-deps \
	--avoid-build-deps \
	--filter-platform "$(rustc -vV | sed -n 's|host: ||p')" \
	> CREDITS

FROM debian:trixie-slim

RUN apt-get update; \
	apt-get install -y --no-install-recommends \
		libssl3 ca-certificates; \
	apt-get clean;

WORKDIR /

COPY --chown=root:root --from=build-env \
	/usr/src/jma-feed-gateway/CREDITS \
	/usr/src/jma-feed-gateway/LICENSE \
	/usr/share/licenses/jma-feed-gateway/

COPY --chown=root:root --from=build-env \
	/usr/src/jma-feed-gateway/target/release/jma-feed-gateway \
	/usr/bin/jma-feed-gateway

CMD ["/usr/bin/jma-feed-gateway"]
