package org.ybennour.duplicatetopic;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ybennour.duplicatetopic.config.ApplicationConfig;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws Exception {
        ApplicationConfig.load();

        Consumer consumer = Server.getConsumer();
        Producer producer = Server.getProducer();

        ConsumerRecords<byte[], byte[]> records = null;

        do {
            records = consumer.poll(Duration.ofMillis(3000L));
            logger.info("poll returned {} records", (Object) records.count());

            if (records.isEmpty()) continue;

            for (ConsumerRecord<byte[], byte[]> record : records) {
                producer.send(new ProducerRecord(ApplicationConfig.getProperty("dest.topic"), Integer.valueOf(record.partition()), Long.valueOf(record.timestamp()), record.key(), record.value()));
            }

            producer.flush();
            logger.info("producer.flush() succeeded");

            consumer.commitSync();
            logger.info("consumer.commitSync() succeeded");

        } while (true);
    }

    private static Consumer getConsumer() {
        HashMap<String, Object> consumerConfig = new HashMap<String, Object>();
        consumerConfig.put("bootstrap.servers", ApplicationConfig.getProperty("src.kafka.bootstrap.servers"));
        consumerConfig.put("group.id", ApplicationConfig.getProperty("group.id"));
        consumerConfig.put("key.deserializer", ByteArrayDeserializer.class);
        consumerConfig.put("value.deserializer", ByteArrayDeserializer.class);
        consumerConfig.put("enable.auto.commit", false);
        consumerConfig.put("auto.offset.reset", ApplicationConfig.getProperty("auto.offset.reset"));
        consumerConfig.put("heartbeat.interval.ms", 3000);
        consumerConfig.put("session.timeout.ms", 10000);

        KafkaConsumer consumer = new KafkaConsumer(consumerConfig);
        Server.closeWhenShutDown((Closeable) consumer);

        consumer.subscribe(Arrays.asList(ApplicationConfig.getProperty("src.topic")));
        consumer.poll(Duration.ofMillis(0L));

        return consumer;
    }

    private static Producer getProducer() throws ExecutionException {
        Properties props = new Properties();
        props.put("bootstrap.servers", ApplicationConfig.getProperty("dest.kafka.bootstrap.servers"));
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", ByteArraySerializer.class);
        props.put("value.serializer", ByteArraySerializer.class);

        KafkaProducer producer = new KafkaProducer(props);
        Server.closeWhenShutDown((Closeable) producer);

        return producer;
    }

    private static void closeWhenShutDown(final Closeable closeable) {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    closeable.close();
                } catch (IOException e) {
                    logger.error("error while closing producer/consumer instance");
                }
            }
        }));
    }

}