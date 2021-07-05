#!/usr/bin/env bash
set -e
set +x

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Public key to be imported
# $4: Alias of the certificate
function create_truststore {
   keytool -keystore "$1" -storepass "$2" -noprompt -alias "$4" -import -file "$3" -storetype PKCS12
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: CA public key to be imported
# $6: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in "$3" -inkey "$4" -name "$6" -password pass:"$2" -out "$1"
}

echo "Preparing truststore for Cruise Control client and server authentication"
STORE=/tmp/cruise-control/client-server.truststore.p12
rm -f "$STORE"
for CRT in /etc/tls-sidecar/cluster-ca-certs/*.crt; do
  ALIAS=$(basename "$CRT" .crt)
  echo "Adding $CRT to truststore $STORE with alias $ALIAS"
  create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
done
echo "Preparing truststore for Cruise Control client and server is complete"

echo "Preparing keystore for Cruise Control client and server"
STORE=/tmp/cruise-control/client-server.keystore.p12
rm -f "$STORE"
create_keystore "$STORE" "$CERTS_STORE_PASSWORD" \
    /etc/tls-sidecar/cc-certs/cruise-control.crt \
    /etc/tls-sidecar/cc-certs/cruise-control.key \
    /etc/tls-sidecar/cluster-ca-certs/ca.crt \
    cruise-control
echo "Preparing keystore for Cruise Control client and server is complete"