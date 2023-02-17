package cn.haohaoli.kafkalearing;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

class ProducerTest {

    static final String TOPIC          = "first";
    static final String BROKER_ADDRESS = "localhost:9092";

    static final Collection<Header> recordHeaders = Arrays.asList(new RecordHeader("header-1", "test_1".getBytes()), new RecordHeader("header-1", "test_2".getBytes()));

    Map<String, Object> initConf() {
        HashMap<String, Object> conf = new HashMap<>();
        /*
         配置生产者如何与 broker建立连接。该参数设置的是初始化参数。
         如果生产者需要连接的是Kafka集群，则这里配置集群中几个broker的地址，而不是全部，当生产者连接上此处指定的broker之后，在通过该连接发现集群中的其他节点
         */
        conf.put("bootstrap.servers", BROKER_ADDRESS);
        /*
         默认值：all。
              acks=0：
                  生产者不等待broker对消息的确认，只要将消息放到缓冲区，就认为消息已经发送完成。
                  该情形不能保证broker是否真的收到了消息，retries配置也不会生效。发送的消息的返回的消息偏移量永远是-1。
              acks=1
                  表示消息只需要写到主分区即可，然后就响应客户端，而不等待副本分区的确认。
                  在该情形下，如果主分区收到消息确认之后就宕机了，而副本分区还没来得及同步该消息，则该消息丢失。
              acks=all
                  首领分区会等待所有的ISR副本分区确认记录。
                  该处理保证了只要有一个ISR副本分区存活，消息就不会丢失。这是Kafka最强的可靠性保证，等效于 acks=-1
         */
        conf.put("acks", "all");
        /*
          retries重试次数

           当消息发送出现错误的时候，系统会重发消息。跟客户端收到错误时重发一样。
           如果设置了重试，还想保证消息的有序性，需要设置 MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION=1
           否则在重试此失败消息的时候，其他的消息可能发送成功了
         */
        conf.put("retries", 3);

        // 要发送信息的key数据的序列化类。设置的时候可以写类名，也可以使用该类的Class对象。
        conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 要发送消息的value数据的序列化类。设置的时候可以写类名，也可以使用该类的Class对象
        conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, BizPartitioner.class.getName());
//        conf.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
        return conf;
    }

    public static class MyProducerInterceptor implements ProducerInterceptor {

        @Override
        public ProducerRecord onSend(ProducerRecord record) {
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

    public static class BizPartitioner implements Partitioner {

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return 0;
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> configs) {

        }
    }

    /**
     * 生产者主要的对象有： {@link KafkaProducer},{@link ProducerRecord}
     * 其中 KafkaProducer 是用于发送消息的类， ProducerRecord 类用于封装Kafka的消息。
     *
     * @see ProducerConfig
     */
    @Test
    void producer() throws ExecutionException, InterruptedException {

        // kafka 生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(initConf());

        /*
            6个构造方法
                public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers)
                public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value)
                public ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers)
                public ProducerRecord(String topic, Integer partition, K key, V value)
                public ProducerRecord(String topic, K key, V value)
                public ProducerRecord(String topic, V value)
         */
        ProducerRecord<String, String> p1 = new ProducerRecord<>(TOPIC, "一个参数","一个参数");
        ProducerRecord<String, String> p2 = new ProducerRecord<>(TOPIC, "1两个参数", "两个参数");
        ProducerRecord<String, String> p3 = new ProducerRecord<>(TOPIC, 0, "两个参数", "两个参数");
        ProducerRecord<String, String> p4 = new ProducerRecord<>(TOPIC, 0, "两个参数", "两个参数", recordHeaders);
        ProducerRecord<String, String> p5 = new ProducerRecord<>(TOPIC, 0, 1L, "两个参数", "两个参数");
        ProducerRecord<String, String> p6 = new ProducerRecord<>(TOPIC, 1, 1L, "两个参数", "两个参数", recordHeaders);


        Future<RecordMetadata> send           = producer.send(p1);
        RecordMetadata         recordMetadata = send.get();
        producer.send(p2, (metadata, exception) -> {
            System.out.println("成功");
        });

        producer.close();
    }

    public void s(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        producer.send(record);
    }

    public void s1(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        Future<RecordMetadata> send = producer.send(record);
        try {
            send.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}

