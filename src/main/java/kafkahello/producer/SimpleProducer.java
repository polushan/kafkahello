package kafkahello.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Enter topic name");
            return;
        }

        String topicName = args[0];
        System.out.println("Producer topic=" + topicName);

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        try(Producer<String, String> producer = new KafkaProducer<>(props)) {

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

            System.out.println("Enter key:value, q - Exit");
            while (true) {
                String input = br.readLine();
                String[] split = input.split(":");

                if ("q".equals(input)) {
                    producer.close();
                    System.out.println("Exit!");
                    return;
                } else {
                    switch (split.length) {
                        case 1:
                            // strategy by round
                            producer.send(new ProducerRecord<>(topicName, split[0]));
                            break;
                        case 2:
                            // strategy by hash
                            producer.send(new ProducerRecord<>(topicName, split[0], split[1]));
                            break;
                        case 3:
                            // strategy by partition
                            producer.send(new ProducerRecord<>(topicName, Integer.valueOf(split[2]), split[0], split[1]));
                            break;
                        default:
                            System.out.println("Enter key:value, q - Exit");
                    }
                }
            }
        }
    }
}
