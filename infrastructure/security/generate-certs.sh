#!/bin/bash
# =============================================================================
# TLS Certificate Generation for Kafka Cluster
# Generates self-signed CA + broker/client certificates for development
# =============================================================================
set -euo pipefail

CERTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/certs"
PASSWORD="dinamo-tls-2024"
VALIDITY=365

mkdir -p "${CERTS_DIR}"
cd "${CERTS_DIR}"

echo "============================================"
echo " Generating TLS Certificates"
echo "============================================"

# --- 1. Certificate Authority ---
echo "▸ Creating Certificate Authority..."
openssl req -new -x509 -keyout ca-key.pem -out ca-cert.pem -days ${VALIDITY} \
    -subj "/CN=DinamoETL-CA/O=DinamoETL/C=BR" \
    -passout pass:${PASSWORD} 2>/dev/null
echo "  ✔ CA certificate generated"

# --- 2. Broker Certificates ---
for broker in kafka-1 kafka-2 kafka-3; do
    echo "▸ Creating certificate for ${broker}..."
    
    # Generate keystore
    keytool -genkey -noprompt -alias ${broker} \
        -keyalg RSA -keysize 2048 \
        -keystore ${broker}.keystore.jks \
        -dname "CN=${broker},O=DinamoETL,C=BR" \
        -storepass ${PASSWORD} -keypass ${PASSWORD} \
        -validity ${VALIDITY} 2>/dev/null
    
    # Generate CSR
    keytool -certreq -alias ${broker} \
        -keystore ${broker}.keystore.jks \
        -file ${broker}.csr \
        -storepass ${PASSWORD} 2>/dev/null
    
    # Sign with CA
    openssl x509 -req -CA ca-cert.pem -CAkey ca-key.pem \
        -in ${broker}.csr -out ${broker}-signed.pem \
        -days ${VALIDITY} -CAcreateserial \
        -passin pass:${PASSWORD} 2>/dev/null
    
    # Import CA cert into keystore
    keytool -import -noprompt -alias ca-cert \
        -file ca-cert.pem \
        -keystore ${broker}.keystore.jks \
        -storepass ${PASSWORD} 2>/dev/null
    
    # Import signed cert
    keytool -import -noprompt -alias ${broker} \
        -file ${broker}-signed.pem \
        -keystore ${broker}.keystore.jks \
        -storepass ${PASSWORD} 2>/dev/null
    
    # Create truststore
    keytool -import -noprompt -alias ca-cert \
        -file ca-cert.pem \
        -keystore ${broker}.truststore.jks \
        -storepass ${PASSWORD} 2>/dev/null
    
    # Cleanup CSR
    rm -f ${broker}.csr ${broker}-signed.pem
    
    echo "  ✔ ${broker} certificates generated"
done

# --- 3. Client Certificate ---
echo "▸ Creating client certificate..."
keytool -import -noprompt -alias ca-cert \
    -file ca-cert.pem \
    -keystore client.truststore.jks \
    -storepass ${PASSWORD} 2>/dev/null
echo "  ✔ Client truststore generated"

# --- 4. Credential file ---
echo "${PASSWORD}" > credentials.txt

echo ""
echo "============================================"
echo " ✔ TLS certificates generated in: ${CERTS_DIR}"
echo "============================================"
echo ""
echo " Files:"
ls -la "${CERTS_DIR}"
