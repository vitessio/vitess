# This Dockerfile should be built from within the accompanying build.sh script.
FROM google/python-runtime

ADD tmp/pkg /app/site-packages
ADD tmp/lib/* /app/lib/
ENV LD_LIBRARY_PATH /app/lib
ENV PYTHONPATH /app/site-packages
