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

wget https://github.com/google/protobuf/archive/v3.0.0-beta-1.tar.gz
tar -xvzf v3.0.0-beta-1.tar.gz
cd protobuf-3.0.0-beta-1
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
