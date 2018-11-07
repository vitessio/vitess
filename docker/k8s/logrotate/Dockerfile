FROM debian:stretch-slim

ADD logrotate.conf /vt/logrotate.conf

ADD rotate.sh /vt/rotate.sh

RUN mkdir -p /vt && \
   apt-get update && \
   apt-get upgrade -qq && \
   apt-get install logrotate -qq --no-install-recommends && \
   apt-get autoremove -qq && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/* && \
   groupadd -r --gid 2000 vitess && \
   useradd -r -g vitess --uid 1000 vitess && \
   chown -R vitess:vitess /vt && \
   chmod +x /vt/rotate.sh

ENTRYPOINT [ "/vt/rotate.sh" ]