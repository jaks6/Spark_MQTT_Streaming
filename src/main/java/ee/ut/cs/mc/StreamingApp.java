package ee.ut.cs.mc;


import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.mqtt.MQTTUtils;

public class StreamingApp {

    public static final long DEFAULT_WINDOW_DURATION = 60;
    public static final long DEFAULT_SLIDE_DURATION = 20;

    public static void main(String[] args) throws InterruptedException {
        String MQTT_BROKER_URL;
        String MQTT_TOPIC;
        Duration windowDuration;
        Duration slideDuration;

        if (args.length < 2){
            if (args[0].equals("--help")) printHelp();
            System.out.println("Usage: StreamingApp mqtt_broker topic. StreamingApp --help for more options");
            return;

        } else {
            MQTT_BROKER_URL = args[0];
            MQTT_TOPIC = args[1];
            final long windowDurationLong = (args.length > 2 ) ? Long.valueOf(args[1]) : DEFAULT_WINDOW_DURATION;
            final long slideDurationLong = (args.length > 3 ) ? Long.valueOf(args[2]) : DEFAULT_SLIDE_DURATION;

            windowDuration = Durations.seconds(windowDurationLong);
            slideDuration = Durations.seconds(slideDurationLong);
        }


        // Create the context with a 10 second batch size
        SparkConf sparkConf = new SparkConf().setAppName("StreamingApp");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));

        JavaReceiverInputDStream<String> readings = MQTTUtils.createStream(
                ssc, MQTT_BROKER_URL, MQTT_TOPIC);


        /* That regex: [^\\d.]+|\\.(?!\\d) means that it is matching one of 2 sub regex on either side of pipe |.
        LHS matches anything except digit or periods whereas RHS means a period which is NOT followed by a digit using negative lookahead */

        JavaDStream<Float> floats = readings
                //convert to float
                .map( x -> Float.valueOf( x.replaceAll("[^\\d.]+|\\.(?!\\d)", "")) )
                // Reduce last 60 seconds of data, every 20 seconds
                .reduceByWindow( (x,y) -> (x+y)/2, windowDuration, slideDuration);
        floats.print();

        ssc.start();
        ssc.awaitTermination();
    }

    private static void printHelp() {
        System.out.println("Usage: StreamingApp mqtt_broker topic window_duration slide_duration");
        System.out.println("mqtt_broker - the url of the mqtt broker to stream data from");
        System.out.println("topic - the url of the mqtt broker to stream data from");
        System.out.println("window_duration - (optional) sliding window duration. Default value is " + DEFAULT_WINDOW_DURATION);
        System.out.println("slide_duration - (optional) sliding window slide duration. Default value is " + DEFAULT_SLIDE_DURATION);
    }
}
