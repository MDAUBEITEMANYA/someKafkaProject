import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class MyKafkaApplication {
    public static void main(String[] args) {
        MyProducer producer = new MyProducer();
        producer.createProducer();

        MyConsumer consumer = new MyConsumer();
        consumer.createConsumer(Constants.FIRST_TOPIC);
        while (true)
            producer.runConsoleProducer(consumer.runConsoleConsumer());
    }
}