# We expect environment variables like the following to be set
#export VAULT_ADDR=https://test:9123
#export VAULT=/path/to/vitess/test/bin/vault-1.6.1
#export VAULT_CACERT=./vault-cert.pem

# For debugging purposes
set -x
TMPFILE=/tmp/setup.sh.tmp.$RANDOM
$VAULT operator init -key-shares=1 -key-threshold=1 | grep ": " | awk '{ print $NF }' > $TMPFILE
export UNSEAL="$(head -1 $TMPFILE)"
export VAULT_TOKEN="$(tail -1 $TMPFILE)"
rm -f $TMPFILE

# Unseal Vault
$VAULT operator unseal $UNSEAL

# Enable secrets engine (v2);  prefix will be /kv
$VAULT secrets enable -version=2 kv

# Enable approles
$VAULT auth enable approle

# Write a custom policy to allow credential access
$VAULT policy write dbcreds dbcreds_policy.hcl

# Load up the db credentials (vttablet -> MySQL) secret
$VAULT kv put kv/prod/dbcreds @dbcreds_secret.json

# Load up the vtgate credentials (app -> vttablet) secret
$VAULT kv put kv/prod/vtgatecreds @vtgatecreds_secret.json

# Configure approle
#   Keep the ttl low, so we can test a refresh
$VAULT write auth/approle/role/vitess secret_id_ttl=10m token_num_uses=0 token_ttl=30s token_max_ttl=0 secret_id_num_uses=4 policies=dbcreds
$VAULT read auth/approle/role/vitess

# Read the role-id of the approle, we need to extract it
export ROLE_ID=$($VAULT read auth/approle/role/vitess/role-id | grep ^role_id | awk '{ print $NF }')

# Get a secret_id for the approle
export SECRET_ID=$($VAULT write auth/approle/role/vitess/secret-id k=v | grep ^secret_id | head -1 | awk '{ print $NF }')

# Echo it back, so the controlling process can read it from the log
echo "ROLE_ID=$ROLE_ID"
echo "SECRET_ID=$SECRET_ID"

