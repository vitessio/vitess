FROM debian:latest@sha256:fac46bff2e02f51425b6e33b0e1169f55dfb053d83511ca28aa50c09fd5ed7a4

ARG MYSQL_VERSION=8.0.34

RUN apt-get update && apt-get -y install curl cmake build-essential libssl-dev libncurses5-dev pkg-config rapidjson-dev

RUN cd /tmp && \
	curl -OL https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-${MYSQL_VERSION}.tar.gz && \
	tar zxvf mysql-${MYSQL_VERSION}.tar.gz

ADD https://gist.githubusercontent.com/vmg/11625faa79574a4d389fb3c04bdd0582/raw/b46389f1d431392cc64d920d4a30306970cff21f/colldump.cc /tmp/mysql-${MYSQL_VERSION}/strings/colldump.cc
RUN echo "MYSQL_ADD_EXECUTABLE(colldump colldump.cc SKIP_INSTALL)\nTARGET_LINK_LIBRARIES(colldump strings)\n" >> /tmp/mysql-${MYSQL_VERSION}/strings/CMakeLists.txt

RUN cd /tmp/mysql-${MYSQL_VERSION} && \
	mkdir build && \
	cd build && \
	cmake -DDOWNLOAD_BOOST=1 -DWITH_BOOST=dist/boost .. && \
	make colldump

RUN mkdir /mysql-collations && /tmp/mysql-${MYSQL_VERSION}/build/runtime_output_directory/colldump /mysql-collations
