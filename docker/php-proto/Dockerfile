FROM vitess/bootstrap:mariadb

USER root

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    rubygems \
    ruby-dev
RUN gem install rake ronn

# copy proto files
COPY proto /vt/src/github.com/youtube/vitess/proto
RUN chown -R vitess:vitess /vt/src/github.com/youtube/vitess/proto

# install necessary packages for proto compiler
WORKDIR /vt
RUN git clone https://github.com/stanley-cheung/Protobuf-PHP
WORKDIR /vt/Protobuf-PHP
RUN rake pear:package version=1.0
RUN pear install Protobuf-1.0.tgz

# Bootstrap Vitess
WORKDIR /vt/src/github.com/youtube/vitess
USER vitess
ENV MYSQL_FLAVOR MariaDB

