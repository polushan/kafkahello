package kafkahello.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Enter topic name, groupId, clientId");
            return;
        }
        final String topicName = args[0];
        final String groupId = args[1];
        final String clientId = args[2];

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("client.id", clientId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topicName));

        System.out.println("Subscribed to topic=" + topicName + ", group=" + groupId + ", clientId=" + clientId);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)

                System.out.printf("offset = %d, key = %s, value = %s,  time = %tT %n",
                        record.offset(), record.key(), record.value(), LocalDateTime.now());
        }
    }
}
