import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKey.class.getSimpleName());

    public static void main(String[] args) {

        log.info("I am a Kafka producer!");

        // set producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:19092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // default Kafka batch size is 16KB
        // properties.setProperty("batch.size", "400"); do not do this in production

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {

            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello World " + i;

                // create a producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // send data -- asynchronous
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a message is successfully sent or an exception is thrown
                        if (e == null) {
                            log.info("Topic: {}\nKey: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                                    recordMetadata.topic(),
                                    key,
                                    recordMetadata.partition(),
                                    recordMetadata.offset(),
                                    recordMetadata.timestamp());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
        }

        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
