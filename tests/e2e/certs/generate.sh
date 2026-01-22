#!/bin/bash
# Generate self-signed certificates for mock KMS mTLS testing
# Usage: generate.sh [output_directory]
# If no directory specified, generates into current working directory.

set -e

# Use provided directory or current directory
OUTPUT_DIR="${1:-$PWD}"
cd "$OUTPUT_DIR"

echo "Generating certificates in: $OUTPUT_DIR"

# CA certificate (used to sign both server and client certs)
echo "Generating CA certificate..."
openssl req -x509 -newkey rsa:4096 -keyout ca.key -out ca.crt -days 365 -nodes \
    -subj "/CN=Mock KMS CA/O=Hippius Test/C=US"

# Server certificate for mock-kms service
echo "Generating server certificate..."
openssl req -newkey rsa:4096 -keyout server.key -out server.csr -nodes \
    -subj "/CN=mock-kms/O=Hippius Test/C=US"

# Create server certificate extensions file
cat > server_ext.cnf << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = mock-kms
DNS.2 = localhost
DNS.3 = toxiproxy
IP.1 = 127.0.0.1
EOF

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days 365 -extfile server_ext.cnf

# Client certificate for hippius-s3 services
echo "Generating client certificate..."
openssl req -newkey rsa:4096 -keyout client.key -out client.csr -nodes \
    -subj "/CN=hippius-client/O=Hippius Test/C=US"

cat > client_ext.cnf << EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
extendedKeyUsage = clientAuth
EOF

openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out client.crt -days 365 -extfile client_ext.cnf

# Cleanup CSR and extension files
rm -f server.csr server_ext.cnf client.csr client_ext.cnf

# Set appropriate permissions
chmod 644 *.crt
chmod 600 *.key

echo "Certificates generated successfully:"
ls -la *.crt *.key

echo ""
echo "Files created:"
echo "  ca.crt, ca.key       - CA certificate and key"
echo "  server.crt, server.key - Server certificate for mock-kms"
echo "  client.crt, client.key - Client certificate for hippius services"
