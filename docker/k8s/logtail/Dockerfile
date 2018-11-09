FROM debian:stretch-slim

ENV TAIL_FILEPATH /dev/null

ADD tail.sh /vt/tail.sh

RUN mkdir -p /vt && \
   apt-get update && \
   apt-get upgrade -qq && \
   apt-get install mysql-client -qq --no-install-recommends && \
   apt-get autoremove -qq && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/* && \
   groupadd -r --gid 2000 vitess && \
   useradd -r -g vitess --uid 1000 vitess && \
   chown -R vitess:vitess /vt && \
   chmod +x /vt/tail.sh

ENTRYPOINT [ "/vt/tail.sh" ]