# This image should be built with $VTTOP as the context dir.
# For example:
#   vitess$ docker build -f docker/publish-site/Dockerfile -t vitess/publish-site .
FROM ruby:2.3

# Install apt dependencies.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        nodejs && \
    rm -rf /var/lib/apt/lists/*

# Install ruby dependencies.
COPY vitess.io/Gemfile /vitess.io/Gemfile
RUN cd /vitess.io && \
    gem install bundler && \
    bundle install

# Expose port for preview-site.sh.
EXPOSE 4000

