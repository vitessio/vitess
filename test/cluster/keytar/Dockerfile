# This Dockerfile should be built from within the accompanying build.sh script.
FROM debian:jessie

RUN apt-get update -y \
 && apt-get install --no-install-recommends -y -q \
    build-essential \
    python2.7 \
    python2.7-dev \
    python-pip \
    git \
    wget \
 && pip install -U pip \
 && pip install virtualenv

WORKDIR /app
RUN virtualenv /env
ADD requirements.txt /app/requirements.txt
RUN /env/bin/pip install -r /app/requirements.txt
ADD keytar.py test_runner.py /app/
ADD static /app/static

ENV USER keytar
ENV DEBIAN_FRONTEND noninteractive

ENV PYTHONPATH /env/lib/python2.7/site-packages
ENV CLOUDSDK_PYTHON_SITEPACKAGES $PYTHONPATH

RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-138.0.0-linux-x86_64.tar.gz
RUN tar -xvf google-cloud-sdk-138.0.0-linux-x86_64.tar.gz
RUN google-cloud-sdk/install.sh --usage-reporting false --path-update true --additional-components kubectl -q
RUN /bin/bash -c "source ~/.bashrc"

EXPOSE 8080
CMD []
ENTRYPOINT ["/env/bin/python", "keytar.py"]

ENV PATH /env/bin:/app/google-cloud-sdk/bin:$PATH

