import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class FibonacciProducer {
    private static Scanner in;
    public static Producer producer;
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }
        String topicName = args[0];
        runProducer(topicName);
    }

    public static void runProducer(String topicName){
        in = new Scanner(System.in);
        System.out.println("Enter message(type exit to quit)");

        producer = getProducer();
        System.out.println("Write down fib number");
        String line = "";
        Integer fibNumber = in.nextInt();

        while(!line.equals("exit")) {
            for (int i = 0; i < fibNumber; i++) {
                line += fibonacci(i) + " ";
            }
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topicName, line);
            producer.send(rec);
            line="";
            fibNumber = in.nextInt();
        }
        in.close();
        producer.close();
    }
    public static KafkaProducer getProducer(){
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(configProperties);
    }
    public static int fibonacci(int n) {
        if (n == 0)
            return 1;
        else if (n == 1)
            return 1;
        else
            return fibonacci(n - 1) + fibonacci(n - 2);
    }
}
