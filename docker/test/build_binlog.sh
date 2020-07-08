#!/bin/bash

bazel=https://github.com/bazelbuild/bazel/releases/download/3.3.1/bazel-3.3.1-linux-x86_64
ripple=https://github.com/google/mysql-ripple.git

#download bazel
curl $bazel --output bazel -L
chmod +x bazel

#download ripple and build it
git clone $ripple
cd mysql-ripple
bazel build :all

# copy rippled and delete dependent directory
cp bazel-bin/rippled ../.
cd -
rm -rf mysql-ripple