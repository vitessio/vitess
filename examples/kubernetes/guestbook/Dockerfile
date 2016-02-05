# This Dockerfile should be built from within the accompanying build.sh script.
FROM debian:jessie

RUN apt-get update -y \
 && apt-get install --no-install-recommends -y -q \
    build-essential \
    python2.7 \
    python2.7-dev \
    python-pip \
    git \
 && pip install -U pip \
 && pip install virtualenv

WORKDIR /app
RUN virtualenv /env
ADD requirements.txt /app/requirements.txt
RUN /env/bin/pip install -r /app/requirements.txt
ADD main.py requirements.txt /app/
ADD static /app/static

EXPOSE 8080
CMD []
ENTRYPOINT ["/env/bin/python", "main.py"]

ADD tmp/pkg /app/pkg
ADD tmp/lib /app/lib
ENV LD_LIBRARY_PATH /app/lib
ENV PYTHONPATH /app/pkg/py-vtdb:/app/pkg/py-mock-1.0.1/lib/python2.7/site-packages:/app/pkg/dist-packages

