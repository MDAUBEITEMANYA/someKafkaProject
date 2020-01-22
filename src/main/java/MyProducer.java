import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer {
    private Producer<Long, String> producer = createProducer();

    public Producer<Long, String> getProducer() {
        return producer;
    }

    public Producer<Long, String> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, Constants.CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

    public void runConsoleProducer(ConsumerRecords<Long, String> consumerRecords) {
        if (consumerRecords != null) {
            for (ConsumerRecord<Long, String> consumerRecord : consumerRecords) {
                ProducerRecord<Long, String> record = new ProducerRecord<>(Constants.SECOND_TOPIC,
                        consumerRecord.value().toUpperCase());
                try {
                    RecordMetadata metadata = producer.send(record).get();
                } catch (ExecutionException | InterruptedException e) {
                    System.out.println("I really need to catch this exc" + e);
                }
            }
        }
    }
}