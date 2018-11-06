FROM debian:stretch-slim

ADD logrotate.conf /vt/logrotate.conf

RUN mkdir -p /vt && \
   apt-get update && \
   apt-get upgrade -qq && \
   apt-get install mysql-client logrotate -qq --no-install-recommends && \
   apt-get autoremove -qq && \
   apt-get clean && \
   rm -rf /var/lib/apt/lists/* && \
   groupadd -r --gid 2000 vitess && \
   useradd -r -g vitess --uid 1000 vitess && \
   chown -R vitess:vitess /vt && \
   echo "0 * * * *	vitess	/usr/sbin/logrotate -s /vt/logrotate.status /vt/logrotate.conf" >> /etc/crontab

CMD ["cron", "-f"]