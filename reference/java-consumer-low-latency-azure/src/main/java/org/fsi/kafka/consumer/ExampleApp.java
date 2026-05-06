package org.fsi.kafka.consumer;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Example: How a CNCB team member would use the reference consumer.
 */
public class ExampleApp {

    private static final Logger log = LoggerFactory.getLogger(ExampleApp.class);

    public static void main(String[] args) {

        Map<String, String> config = Map.of(
            "bootstrap.servers", "kafka.fsi.internal:9092",
            "schema.registry.url", "https://schema.fsi.internal",
            "schema.registry.basic.auth.user.info",
                System.getenv("SR_API_KEY") + ":" + System.getenv("SR_API_SECRET"),
            "sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username='" + System.getenv("KAFKA_API_KEY") + "' " +
                "password='" + System.getenv("KAFKA_API_SECRET") + "';",
            "topics", "cncb.core.v1.account-transaction",
            "group.id", "sa-cncb-consumer-example",
            "client.rack", "us-east-1"
        );

        // The handler is where your business logic lives
        FsiConsumer consumer = new FsiConsumer(config, (key, record) -> {
            String txnId = record.get("transaction_id").toString();
            String type = record.get("transaction_type").toString();
            String channel = record.get("channel").toString();

            log.info("Processing transaction: id={}, type={}, channel={}, key={}",
                    txnId, type, channel, key);

            // Your business logic here:
            // - Update account balance
            // - Trigger downstream processing
            // - Write to database
        });

        // Blocks until SIGTERM or consumer.shutdown() is called
        consumer.start();
    }
}
