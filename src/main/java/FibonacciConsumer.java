import jnr.ffi.annotations.In;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class FibonacciConsumer {
    private static String TOPIC = "test";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";
    public static Consumer<Long, String> consumer;
    private static int m=0;
    public static void main(String[] args) throws InterruptedException {
        try{
            TOPIC = args[0];
            if(Integer.parseInt(args[1])>=0){
                runConsumer(args[0], args[1]);
            }
            else throw new NumberFormatException();
        }
        catch(NumberFormatException e){
            System.out.println("You have entered wrong number");
        }
    }

    private static Consumer<Long, String> createConsumer(String topic) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "FibonacciConsumer2");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        consumer = new KafkaConsumer<>(props);
        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }


    static void runConsumer(String topic, String param) throws InterruptedException {
        consumer = createConsumer(topic);
        m = Integer.parseInt(param);

        while (true) {
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);

           checkForInput(m,consumerRecords);
            consumer.commitAsync();
        }
    }

    public static void checkForInput(Integer param, ConsumerRecords<Long,String> consumerRecords){
        int count = 1;
        final int giveUp = 100;
        int noRecordsCount = 0;

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
