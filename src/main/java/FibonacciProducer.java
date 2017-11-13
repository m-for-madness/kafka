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
    public static String topicName="";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Please specify 1 parameters ");
            System.exit(-1);
        }
        topicName = args[0];
        runProducer();
    }

    public static void runProducer(){
        producer = getProducer();
        inputRecords(producer);
        producer.close();
    }

    public static void inputRecords(Producer producer){
        in = new Scanner(System.in);
        System.out.println("Write down quantity of fibonacci numbers (type exit to quit)");
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
    }
    public static KafkaProducer getProducer(){
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.LongSerializer");
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
