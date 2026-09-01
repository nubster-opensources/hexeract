#!/usr/bin/env bash
#
# Regenerate the disposable TLS material used by tests/tls.rs.
#
# Overwrites ca.pem, server.pem, server-key.pem and client.p12 in place, then
# verifies the chain it just built. Every intermediate, including the
# authority's own private key, is deleted on exit: this material is meant to be
# regenerated wholesale rather than extended.
#
# Read README.md before changing any subject, SAN or password. They are
# mirrored in tests/harness.rs and in the broker configuration it writes.

set -euo pipefail

# Git Bash rewrites any argument that looks like an absolute POSIX path into a
# Windows one, which turns "/CN=..." into "C:/Program Files/Git/CN=...". Both
# variables switch that off and are ignored on every other platform. The
# workspace below stays a relative path for the same reason: the OpenSSL build
# shipped for Windows would not find a "/tmp/..." directory.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

readonly CA_SUBJECT="/CN=Hexeract RabbitMQ test CA"
readonly SERVER_SUBJECT="/CN=localhost"
readonly CLIENT_SUBJECT="/CN=hexeract-test-client"
readonly SERVER_ALT_NAMES="subjectAltName=DNS:localhost,IP:127.0.0.1"

# Matches the literal in tests/harness.rs::client_tls_config. Not a secret: see
# README.md, "The private key in this directory is not a leak".
readonly CLIENT_BUNDLE_PASSWORD="hexeract-test"

# Ten years. Long enough that expiry never explains a red CI run, short enough
# to stay a finite lifetime rather than a de facto permanent certificate.
readonly VALIDITY_DAYS=3650

readonly KEY_BITS=2048

readonly WORKSPACE="regenerate-workspace"

cd "$(dirname "$0")"

rm -rf "${WORKSPACE}"
mkdir "${WORKSPACE}"
trap 'rm -rf "${WORKSPACE}"' EXIT

echo "Generating the certificate authority"
openssl req -x509 -newkey "rsa:${KEY_BITS}" -nodes \
    -keyout "${WORKSPACE}/ca-key.pem" \
    -out ca.pem \
    -days "${VALIDITY_DAYS}" \
    -subj "${CA_SUBJECT}" 2>/dev/null

echo "Generating the broker certificate"
openssl req -newkey "rsa:${KEY_BITS}" -nodes \
    -keyout server-key.pem \
    -out "${WORKSPACE}/server.csr" \
    -subj "${SERVER_SUBJECT}" 2>/dev/null
printf '%s\n' "${SERVER_ALT_NAMES}" > "${WORKSPACE}/server-ext.cnf"
openssl x509 -req \
    -in "${WORKSPACE}/server.csr" \
    -CA ca.pem -CAkey "${WORKSPACE}/ca-key.pem" \
    -CAserial "${WORKSPACE}/ca.srl" -CAcreateserial \
    -out server.pem \
    -days "${VALIDITY_DAYS}" \
    -extfile "${WORKSPACE}/server-ext.cnf" 2>/dev/null

# No extension file for the client: the certificate is deliberately bare, and
# README.md explains why the broker accepts it anyway.
echo "Generating the client certificate"
openssl req -newkey "rsa:${KEY_BITS}" -nodes \
    -keyout "${WORKSPACE}/client-key.pem" \
    -out "${WORKSPACE}/client.csr" \
    -subj "${CLIENT_SUBJECT}" 2>/dev/null
openssl x509 -req \
    -in "${WORKSPACE}/client.csr" \
    -CA ca.pem -CAkey "${WORKSPACE}/ca-key.pem" \
    -CAserial "${WORKSPACE}/ca.srl" -CAcreateserial \
    -out "${WORKSPACE}/client.pem" \
    -days "${VALIDITY_DAYS}" 2>/dev/null

echo "Bundling the client identity"
openssl pkcs12 -export \
    -inkey "${WORKSPACE}/client-key.pem" \
    -in "${WORKSPACE}/client.pem" \
    -out client.p12 \
    -passout "pass:${CLIENT_BUNDLE_PASSWORD}"

echo "Verifying"
openssl verify -CAfile ca.pem server.pem
openssl verify -CAfile ca.pem "${WORKSPACE}/client.pem"
openssl pkcs12 -in client.p12 -nokeys -passin "pass:${CLIENT_BUNDLE_PASSWORD}" \
    -out "${WORKSPACE}/bundle-readback.pem"

echo "Done. ca.pem, server.pem, server-key.pem and client.p12 are regenerated."
