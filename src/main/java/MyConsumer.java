import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

class MyConsumer {
    private Consumer<Long, String> consumer = createConsumer(Constants.FIRST_TOPIC);

    public Consumer<Long, String> getConsumer() {
        return consumer;
    }

    Consumer<Long, String> createConsumer(String topicName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_BROKERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.OFFSET_RESET_EARLIER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID_CONFIG);
        org.apache.kafka.clients.consumer.Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        return consumer;
    }

    ConsumerRecords<Long, String> runConsoleConsumer() {
        ConsumerRecords<Long, String> consumerRecordsToSend = null;
        while (Constants.RUNNING) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.of(3, ChronoUnit.SECONDS));
            if (consumerRecords.count() == 0) {
                break;
            }
            consumer.commitAsync();
            consumerRecordsToSend = consumerRecords;
        }
        return consumerRecordsToSend;
    }
}