# Dockerfile for generating the keytar image. See README.md for more information.
FROM debian:jessie

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update -y \
 && apt-get install --no-install-recommends -y -q \
    apt-utils \
    apt-transport-https \
    build-essential \
    curl \
    python2.7 \
    python2.7-dev \
    python-pip \
    git \
    wget \
 && pip install -U pip \
 && pip install virtualenv

RUN echo "deb https://packages.cloud.google.com/apt cloud-sdk-jessie main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN apt-get update -y && apt-get install -y google-cloud-sdk && apt-get install -y kubectl

WORKDIR /app
RUN virtualenv /env
ADD requirements.txt /app/requirements.txt
RUN /env/bin/pip install -r /app/requirements.txt
ADD keytar.py test_runner.py /app/
ADD static /app/static

ENV USER keytar

ENV PYTHONPATH /env/lib/python2.7/site-packages
ENV CLOUDSDK_PYTHON_SITEPACKAGES $PYTHONPATH

RUN /bin/bash -c "source ~/.bashrc"

EXPOSE 8080
CMD []
ENTRYPOINT ["/env/bin/python", "keytar.py"]

ENV PATH /env/bin:$PATH

