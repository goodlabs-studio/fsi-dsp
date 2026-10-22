#!/usr/lpp/IBM/foz/v1r1/bin/bash
# Kafka Connect Distributed Starter Script

# Set environment
export LIBPATH=$LIBPATH:/usr/lpp/wsmq/mqm/V91R2103/java/lib
export CLASSPATH=/u/kafconn/confluent/confluent-connect-7.5.2-zOS/plugins:/u/kafconn/confluent/confluent-connect-7.5.2-zOS/share/java

# ---> ADD THIS LINE <---
# Configure Java to use the RACF keyring for SSL connections
# Replace 'app_userid' and 'YourAppKeyring' with your actual values
export KAFKA_OPTS="-Djavax.net.ssl.trustStore=safkeyring://app_userid/YourAppKeyring -Djavax.net.ssl.trustStoreType=JCERACFKS"

# Change to bin directory
cd /u/kafconn/confluent/confluent-connect-7.5.2-zOS/bin

# Start Kafka Connect
exec ./connect-distributed ../etc/kafka/connect-distributed.properties
