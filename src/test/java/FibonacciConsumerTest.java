import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

public class FibonacciConsumerTest {
    private MockConsumer<Long, String> consumer;
    String TOPIC = "test";
    @Before
    public void setUp() throws Exception {
        consumer  = new MockConsumer<Long, String>(OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void testConsumer() throws Exception {
        FibonacciConsumer myTestConsumer = new FibonacciConsumer();
        myTestConsumer.consumer = consumer;

        consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0)));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("my_topic", 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        consumer.addRecord(new ConsumerRecord<Long, String>(TOPIC,
                0, 0L, null, "1 1 2 3"));
        consumer.addRecord(new ConsumerRecord<Long, String>(TOPIC, 0,
                1L, null, "1 1 2 3 5"));
        consumer.addRecord(new ConsumerRecord<Long, String>(TOPIC, 0,
                2L, null, "1 1"));
        consumer.addRecord(new ConsumerRecord<Long, String>(TOPIC, 0,
                3L, null, "1 1 2"));
        consumer.addRecord(new ConsumerRecord<Long, String>(TOPIC, 0,
                4L, null, "1 1 2 3 5"));

        myTestConsumer.consume();
    }
}
