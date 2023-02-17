package cn.haohaoli.kafkalearing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class ConsumerTest1 {

    Map<String,Object> initConf() {
        HashMap<String, Object> conf = new HashMap<>();
        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ProducerTest.BROKER_ADDRESS);
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "group_test");
        /*
            latest, earliest, none

            当Kafka中没有初始偏移量或当前偏移量在服务器中不存在（如，数据被删除了），该如何处理？
                earliest：自动重置偏移量到最早的偏移量
                latest：自动重置偏移量为最新的偏移量
                none：如果消费组原来的（previous）偏移量不存在，则向消费者抛异常
                anything：向消费者抛异常
         */
        conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        /*
            这个参数用来设定 KafkaProducer 对应的客户端 id ， 默认值为“” 。 如果客户端不设置， 则 KafkaProducer 会
            自动生成一个非空字符串，内容形式如“ producer-I”“producer -2 ” ，即字符串“ producer-"与数字的拼接
         */
        conf.put(ConsumerConfig.CLIENT_ID_CONFIG, "haha");
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return conf;
    }


    @Test
    void consumer() {
        // 先订阅在消费
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(initConf());
        consumer.subscribe(Collections.singleton(ProducerTest.TOPIC));

        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1));
            for (ConsumerRecord<String, String> record : poll) {
                System.out.println("topic: " + record.topic());
                System.out.println("partition: " + record.partition());
                System.out.println("offset: " + record.offset());
                System.out.println("offset: " + record.headers());
                System.out.println("timestamp: " + record.timestamp());
                System.out.println("key: " + record.key());
                System.out.println("value: " + record.value());
            }
        }
    }
}

