package cn.haohaoli.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author lwh
 */
public class ProducerTest {

    // 主题(topic)
    static final String TOPIC = "hello";

    public static void main(String[] args) {

        // 1. 配置生产者参数
        Properties config = new Properties();
        config.put("bootstrap.servers", "localhost:9093,localhost:9094,localhost:9095");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2. 创建 KafkaProducer 对象. 对应的是生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        // 3. 创建 ProducerRecord 对象. 包含消息与对应的主题信息
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC,0, "hello Kafka","hello Kafka");

        // 4. 发送消息(发后即忘模式)
        producer.send(record);

        // 4. 发送消息(异步模式)
        producer.send(new ProducerRecord<>(TOPIC,1, "hello Kafka async","hello Kafka async"), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("-------发送失败-------");
                    exception.printStackTrace();
                } else {
                    System.out.println("-------发送成功-------");
                    System.out.println("topic: " + metadata.topic());
                    System.out.println("offset: " + metadata.offset());
                    System.out.println("timestamp: " + metadata.timestamp());
                    System.out.println("partition: " + metadata.partition());
                }
            }
        });

        // 4. 发送消息(同步模式)
        try {
            Future<RecordMetadata> result = producer.send(new ProducerRecord<>(TOPIC,2,"hello Kafka sync", "hello Kafka sync"));
            RecordMetadata metadata = result.get();
            System.out.println("-------发送成功-------");
            System.out.println("topic: " + metadata.topic());
            System.out.println("offset: " + metadata.offset());
            System.out.println("timestamp: " + metadata.timestamp());
            System.out.println("partition: " + metadata.partition());
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("-------发送失败-------");
            e.printStackTrace();
        }

        // 5. 关闭生产者
        producer.close();
    }

}
