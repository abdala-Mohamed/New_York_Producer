package kafka.New_York_Producer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class New_York_Kafka_Prod {
    Properties  properties = null;
    KafkaProducer<String,String> producer = null;
    public New_York_Kafka_Prod(){
        properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//how to send the key bytes in which format
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());//how to send the value bytes in which format

        producer = new KafkaProducer(properties);
    }
    public void producer(String topic_name,String Json_record){

            String key = "";
            ProducerRecord<String,String> record =
                    new ProducerRecord<String,String>(topic_name,key,Json_record);
            //by providing key we are making sure the record is going to the same partition
            producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e ==null){

                    }
                    else{
                        System.out.println(e.toString());
                    }
                }
            });//add .get here to make it synchronus

    }
    public void close_produce(){
        producer.close();
    }

}
