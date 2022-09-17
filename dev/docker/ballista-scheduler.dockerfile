ARG VERSION=0.8.0
FROM ballista:$VERSION

# Scheduler UI
RUN apt -y install nodejs npm nginx
RUN npm install --global yarn
RUN npm install --save node-releases
RUN mkdir /tmp/ballista-ui
WORKDIR /tmp/ballista-ui

COPY ballista/ui/scheduler ./
RUN yarn
RUN yarn build

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full

CMD ["/scheduler"]