package cn.haohaoli.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;

/**
 * @author lwh
 */
public class ProducerInterceptorTest {

    // 主题
    static final String TOPIC = "interceptor";

    public static void main(String[] args) {

        // 1. 配置生产者参数
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094,localhost:9095");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, String.join(",", FirstInterceptor.class.getName(), SecondInterceptor.class.getName()));

        // 2. 创建 KafkaProducer 对象. 对应的是生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        // 3. 创建 ProducerRecord 对象. 包含消息与对应的主题信息
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "hello Kafka");

        // 4. 发送消息(发后即忘模式)
        producer.send(record);

        // 5. 关闭生产者
        producer.close();
    }

    public static class FirstInterceptor implements ProducerInterceptor<String, String> {

        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            System.out.println("FirstInterceptor -> onSend");
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            System.out.println("FirstInterceptor -> onAcknowledgement");
        }

        @Override
        public void close() {
            System.out.println("FirstInterceptor -> close");
        }

        @Override
        public void configure(Map<String, ?> configs) {
            System.out.println("FirstInterceptor -> configure");
        }
    }

    public static class SecondInterceptor implements ProducerInterceptor<String, String> {

        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            System.out.println("SecondInterceptor -> onSend");
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            System.out.println("SecondInterceptor -> onAcknowledgement");
        }

        @Override
        public void close() {
            System.out.println("SecondInterceptor -> close");
        }

        @Override
        public void configure(Map<String, ?> configs) {
            System.out.println("SecondInterceptor -> configure");
        }
    }
}
