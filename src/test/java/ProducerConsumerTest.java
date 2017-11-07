import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import static org.junit.Assert.*;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ProducerConsumerTest {

    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 2, "messages");

    @Test
    public void testProducerAndConsumer() throws ExecutionException, InterruptedException {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(configProperties);
        producer.send(new ProducerRecord<>("messages", 0, 0, "message0")).get();
        producer.send(new ProducerRecord<>("messages", 0, 1, "message1")).get();
        producer.send(new ProducerRecord<>("messages", 1, 2, "message2")).get();
        producer.send(new ProducerRecord<>("messages", 1, 3, "message3")).get();


        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");

        final CountDownLatch latch = new CountDownLatch(4);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
            kafkaConsumer.subscribe(Collections.singletonList("messages"));
            try {
                while (true) {
                    ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<Integer, String> record : records) {
                        System.out.format("consuming from topic =  %d, partition =  %d, offset =  %d, key =  %d, value =  %d",
                                record.topic(), record.partition(), record.offset(), record.key(), record.value());
                        latch.countDown();
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });

        assertThat(latch.await(90, TimeUnit.SECONDS)).isTrue();
    }
}
