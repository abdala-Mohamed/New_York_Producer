package kafka.New_York_Producer;


import java.io.FileNotFoundException;

public class New_York_Producer {

    public static void main(String args[]) throws FileNotFoundException, InterruptedException {
        String file_path = "D:\\personal project\\311_Service_Requests_from_2010_to_Present.csv";
        Json_producer json_producer = new Json_producer(file_path);
        String status = json_producer.start();
        System.out.println(status);
        //gdf



    }

}
