package org.fsi.kafka.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Example: How a CNCB team member would use the reference producer.
 * This is NOT production code — it demonstrates the integration pattern.
 */
public class ExampleApp {

    public static void main(String[] args) throws Exception {

        // In production, these come from environment variables / Vault / Spring config
        Map<String, String> config = Map.of(
            "bootstrap.servers", "kafka.fsi.internal:9092",  // Consul DNS
            "schema.registry.url", "https://schema.fsi.internal",
            "schema.registry.basic.auth.user.info", System.getenv("SR_API_KEY") + ":" + System.getenv("SR_API_SECRET"),
            "sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username='" + System.getenv("KAFKA_API_KEY") + "' " +
                "password='" + System.getenv("KAFKA_API_SECRET") + "';",
            "topic.name", "cncb.core.v1.account-transaction",
            "client.id", "cncb-producer-example"
        );

        // Load the Avro schema
        Schema schema = new Schema.Parser().parse(
            Files.readString(Path.of("../../schemas/cncb-account-transaction.avsc")));

        try (FsiProducer producer = new FsiProducer(config)) {

            // Build an Avro record
            GenericRecord txn = new GenericData.Record(schema);
            txn.put("transaction_id", UUID.randomUUID().toString());
            txn.put("account_number", "1234567890");
            txn.put("member_id", "M-001");
            txn.put("member_name", "Jane Doe");
            txn.put("transaction_type", "DEBIT");
            // txn.put("amount", ...) -- decimal requires ByteBuffer encoding
            txn.put("currency", "USD");
            txn.put("channel", "POS");
            txn.put("timestamp", Instant.now().toEpochMilli());
            txn.put("source_system", "CNCB-CORE");

            // Send with account_number as key (ensures partition affinity)
            producer.send("1234567890", txn);
            producer.flush();

            System.out.println("Produced 1 record to cncb.core.v1.account-transaction");
        }
    }
}
