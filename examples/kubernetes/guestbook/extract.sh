# Don't run this. It is only used as part of build.sh.

set -e

# Collect all the local Python libs we need.
cp -R $VTTOP/py/* /out/pkg/
mkdir /out/pkg/google
cp -R /usr/local/lib/python2.7/dist-packages/* /out/pkg/
for pypath in $(find $VTROOT/dist -name site-packages); do
  cp -R $pypath/* /out/pkg/
done

# We also need the grpc libraries.
cp /usr/local/lib/libgrpc.so.0 /usr/local/lib/libgpr.so.0 /out/lib/
