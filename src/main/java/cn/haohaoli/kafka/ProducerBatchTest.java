package cn.haohaoli.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author lwh
 */
public class ProducerBatchTest {

    static final String TOPIC = "batch";

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094,localhost:9095");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>(TOPIC, "hello Kafka async" + i), (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("-------发送失败-------");
                    exception.printStackTrace();
                } else {
                    System.out.println("-------发送成功-------");
                }
            });
        }

        producer.close();
    }

}
