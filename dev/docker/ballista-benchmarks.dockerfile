ARG VERSION=0.8.0
FROM ballista:$VERSION

ADD benchmarks/run.sh /
RUN mkdir /queries
COPY benchmarks/queries/ /queries/

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/run.sh"]