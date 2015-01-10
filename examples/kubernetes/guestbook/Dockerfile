# This Dockerfile should be built from within the accompanying build.sh script.
FROM google/python-runtime

ADD tmp /app/site-packages
ENV PYTHONPATH /app/site-packages
