import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertFalse;

public class FibonacciProducerTest {
    MockProducer<String, String> producer;
    String TOPIC = "test";
    @Before
    public void setUp() {
        producer = new MockProducer<String, String>(
                true, new StringSerializer(), new StringSerializer());
    }

    @Test
    public void testProducer() throws IOException {
        FibonacciProducer myTestKafkaProducer = new FibonacciProducer();
        myTestKafkaProducer.producer = producer;
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>(TOPIC, "mykey", "myvalue0");
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>(TOPIC, "mykey", "myvalue1");

        Future<RecordMetadata> md3 = producer.send(record1);
        Future<RecordMetadata> md4 = producer.send(record2);
        producer.flush();
        assertTrue("Requests should be completed.", md3.isDone() && md4.isDone());
    }

}
