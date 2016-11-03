# Don't run this. It is only used as part of build.sh.

set -e

# Collect all the local Python libs we need.
mkdir -p /out/pkg/py-vtdb
cp -R $VTTOP/py/* /out/pkg/py-vtdb/
cp -R /usr/local/lib/python2.7/dist-packages /out/pkg/
cp -R /vt/dist/py-* /out/pkg/

# We also need the grpc libraries.
cp /usr/local/lib/libgrpc.so /out/lib/
