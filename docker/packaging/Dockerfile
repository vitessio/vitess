FROM vitess/base

USER root

# Install gem and use gem to install fpm
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
   build-essential \
   ruby-dev \
   rubygems \
   rpm \
 && rm -rf /var/lib/apt/lists/* \
 && gem install --no-ri --no-rdoc fpm

RUN mkdir /vt/packaging

COPY docker/packaging/* /vt/packaging/

ENTRYPOINT ["/bin/bash", "/vt/packaging/package_vitess.sh"]
