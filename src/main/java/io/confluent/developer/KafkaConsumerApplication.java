package io.confluent.developer;


import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerApplication {

    private volatile boolean keepConsuming = true;
    private ConsumerRecordsHandler<String, String> fileRecordsHandler;
    private ConsumerRecordsHandler<String, String> statisticsRecordsHandler;
    private Consumer<String, String> consumer;

    public KafkaConsumerApplication(final Consumer<String, String> consumer,
                                    final ConsumerRecordsHandler<String, String> fileRecordsHandler,
                                    final ConsumerRecordsHandler<String, String> statisticsRecordsHandler
                                    ) {
        this.consumer = consumer;
        this.fileRecordsHandler = fileRecordsHandler;
        this.statisticsRecordsHandler = statisticsRecordsHandler;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "This program takes one argument: the path to an environment configuration file.");
        }

        final Properties consumerAppProps = KafkaConsumerApplication.loadProperties(args[0]);
        final String filePath = consumerAppProps.getProperty("file.path");
        final Consumer<String, String> consumer = new KafkaConsumer<>(consumerAppProps);
        final ConsumerRecordsHandler<String, String> fileRecordsHandler = new FileWritingRecordsHandler(Paths.get(filePath));
        final ConsumerRecordsHandler<String, String> statisticsRecordHandler = new StatisticsRecordsHandler(consumerAppProps);
        final KafkaConsumerApplication consumerApplication = new KafkaConsumerApplication(consumer, fileRecordsHandler, statisticsRecordHandler);

        Runtime.getRuntime().addShutdownHook(new Thread(consumerApplication::shutdown));

        consumerApplication.runConsume(consumerAppProps);
    }

    public void runConsume(final Properties consumerProps) {
        try {
            String topicA = consumerProps.getProperty("input.topic.name");
            String topicB = consumerProps.getProperty("output.topic.name");

            consumer.subscribe(Arrays.asList(topicA, topicB));
            //consumer.subscribe(Collections.singletonList(consumerProps.getProperty("input.topic.name")));

            while (keepConsuming) {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                if (consumerRecords.count() > 0) {
                    System.out.println("Check consumerRecords size =" + consumerRecords.count());
                    //consumerRecords.forEach(System.out::println);
                    //fileRecordsHandler.process(consumerRecords);
                    statisticsRecordsHandler.process(consumerRecords);
                }
            }
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        keepConsuming = false;
    }

    public static Properties loadProperties(String fileName) throws IOException {
        final Properties props = new Properties();
        final FileInputStream input = new FileInputStream(fileName);
        props.load(input);
        input.close();
        return props;
    }
}
