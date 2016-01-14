#!/bin/bash

# This script downloads and installs the protobuf library, for
# C++ and python, in the root of the image. It assumes we're running
# as root in the image.
set -ex

# protobuf_dist can be empty, in which case we just install to the default paths
protobuf_dist="$1"
if [ "$protobuf_dist" != "" ]; then
  cd $protobuf_dist
fi

# this is the default working beta version on Linux, beta-2 doesn't work here
protobuf_beta_version=1

# on OSX beta-1 doesn't work, it has to be built in version beta-2
if [ `uname -s` == "Darwin" ]; then
  protobuf_beta_version=2
fi

wget https://github.com/google/protobuf/archive/v3.0.0-beta-$protobuf_beta_version.tar.gz
tar -xvzf v3.0.0-beta-$protobuf_beta_version.tar.gz
cd protobuf-3.0.0-beta-$protobuf_beta_version

./autogen.sh
if [ "$protobuf_dist" != "" ]; then
  ./configure --prefix=$protobuf_dist
else
  ./configure
fi
make
make install
cd python
python setup.py build --cpp_implementation
if [ "$protobuf_dist" != "" ]; then
  python setup.py install --cpp_implementation --prefix=$protobuf_dist
else
  python setup.py install --cpp_implementation
fi
