public class MyKafkaApplication {

    public static void main(String[] args) {

        MyProducer producer = new MyProducer();
        MyConsumer consumer = new MyConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("LOl");
            producer.getProducer().close();
            consumer.getConsumer().close();
        }));

        while (true)
            producer.runConsoleProducer(consumer.runConsoleConsumer());
    }
}


