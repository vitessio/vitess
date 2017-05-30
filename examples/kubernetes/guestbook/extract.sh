# Copyright 2017 Google Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Don't run this. It is only used as part of build.sh.

set -e

# Collect all the local Python libs we need.
mkdir -p /out/pkg/py-vtdb
cp -R $VTTOP/py/* /out/pkg/py-vtdb/
cp -R /usr/local/lib/python2.7/dist-packages /out/pkg/
cp -R /vt/dist/py-* /out/pkg/

# We also need the grpc libraries.
cp /usr/local/lib/libgrpc.so /out/lib/
