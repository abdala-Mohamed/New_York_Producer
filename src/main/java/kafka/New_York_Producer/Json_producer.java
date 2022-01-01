package kafka.New_York_Producer;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;
import javax.json.Json;
import javax.json.JsonObjectBuilder;
import java.util.concurrent.TimeUnit;

public class Json_producer {
    JsonObjectBuilder json = null;
    FileInputStream inputStream = null;
    Scanner sc = null;
    New_York_Kafka_Prod kafka_prod = null;

    public Json_producer(String file_path) throws FileNotFoundException {
    this.json = Json.createObjectBuilder();
    inputStream = new FileInputStream(file_path);
    sc = new Scanner(inputStream, "UTF-8");
    kafka_prod = new New_York_Kafka_Prod();
    }

    public String start() throws InterruptedException {

        String schema = "";

        if(sc.hasNextLine()){
            schema =  sc.nextLine();

        }

        else{

            return "no data";
        }

        String [] metadata = schema.split(",");

        while (sc.hasNextLine()) {

            String line = sc.nextLine();
            String data[] =line.split(",");


            for(int i=0;i<metadata.length ;i++){
                json.add(metadata[i],data[i]);
            }
            //send data here
            TimeUnit.SECONDS.sleep(10);

            kafka_prod.producer("New_York_Data",json.build().toString());


            //System.out.println(json.build().toString());

        }
        kafka_prod.close_produce();
        return "finished sending data";


        }

    }
