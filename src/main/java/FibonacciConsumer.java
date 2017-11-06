import jnr.ffi.annotations.In;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class FibonacciConsumer {
    private final static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";


    public static void main(String[] args) throws InterruptedException {
        runConsumer(args[0]);
    }

    private static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "FibonacciConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }


    static void runConsumer(String param) throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        int count = 1;
        final int giveUp = 100;
        int noRecordsCount = 0;
        int m = Integer.parseInt(param);
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            for (ConsumerRecord cr : consumerRecords) {
                if (count % m == 0) {
                    System.out.println(cr.value());
                } else
                    System.out.println(parseLine(cr.value().toString()));

                count++;
            }
            consumer.commitAsync();
        }
    }

    public static Integer parseLine(String line) {
        String[] s = line.split(" ");
        Integer sum = 0;
        for (int i = 0; i < s.length; i++) {
            sum += Integer.parseInt(s[i]);
        }
        return sum;
    }
}
