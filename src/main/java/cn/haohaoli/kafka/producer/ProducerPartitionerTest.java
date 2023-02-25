package cn.haohaoli.kafka.producer;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * @author lwh
 */
public class ProducerPartitionerTest {

    static final String TOPIC = "partitioner";

    static {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094,localhost:9095");
        try (AdminClient adminClient = AdminClient.create(config)) {
            TopicListing topic = adminClient.listTopics().namesToListings().get().get(TOPIC);
            if (topic == null) {
                System.out.println("创建主题");
                NewTopic newTopic = new NewTopic(TOPIC, 3, (short) 2);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094,localhost:9095");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, Interceptor.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < 20; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "hello Kafka" + i, "hello Kafka" + i));
            }
        }
    }

    public static class CustomPartitioner implements Partitioner {

        private static final AtomicInteger counter = new AtomicInteger();

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            System.out.println("CustomPartitioner -> partition");
            if (key == null || counter.getAndIncrement() % 5 == 0) {
                return 0;
            }
            return Utils.toPositive(Utils.murmur2(keyBytes)) % cluster.partitionsForTopic(topic).size();
        }

        @Override
        public void close() {
            System.out.println("CustomPartitioner -> close");
        }

        @Override
        public void configure(Map<String, ?> configs) {
            System.out.println("CustomPartitioner -> configure");
        }
    }

    public static class Interceptor implements ProducerInterceptor<String, String> {

        private static final Map<Integer, Integer> map = new HashMap<>();

        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            int partition = metadata.partition();
            map.put(partition, map.getOrDefault(partition, 0) + 1);
        }

        @Override
        public void close() {
            map.forEach((k, v) -> System.out.println("分区: " + k + ",消息: " + v + "条"));
        }

        @Override
        public void configure(Map<String, ?> configs) {
            System.out.println("FirstInterceptor -> configure");
        }
    }

}
