# This is an include file used by the other scripts in this directory.

if [ -z "$KUBECTL" ]; then
  echo 'Please set KUBECTL env var to point to kubectl or kubectl.sh'
  exit 1
fi
