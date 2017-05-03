import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.AbstractProcessor;
import scala.Long;

import java.io.StringReader;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by DmitriyBrosalin on 26/04/2017.
 */
public class JavaKafkaStreams {

    private String brokers;
    private String topicReadFrom;
    private String topicSaveTo;
    private JsonParser parser = new JsonParser();
    private Gson gson = new GsonBuilder().create();

    public JavaKafkaStreams(String brokerList, String topicReadFrom, String topicSaveTo){
        this.brokers = brokerList;
        this.topicReadFrom = topicReadFrom;
        this.topicSaveTo = topicSaveTo;
    }

    private Properties initializeStreams(String brokerList){
        Properties properties = new Properties();
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "BROSALIN_STREAMS_APPLICATION");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return properties;
    }

    private String parseSimpleMessageGetHost(String jsonSimpleMessage){
        return jsonSimpleMessage.split(" ")[0];
    }

    public void runStream(){
        Properties properties = initializeStreams(brokers);
        KStreamBuilder myStreamBuilder = new KStreamBuilder();
        Serde<String> stringSerde = Serdes.String();
        KTable<Windowed<String>, String> windowedTable = myStreamBuilder.stream(stringSerde, stringSerde, topicReadFrom)
                .map((k, v) -> new KeyValue<>(parseSimpleMessageGetHost(v), v))
                .reduceByKey(
                        (k,v) -> k+v,
                        TimeWindows.of("TempWindow", 10000L));
        windowedTable.mapValues(v ->
            Arrays.stream(v.split(" "))
                    .distinct()
                    .collect(Collectors.joining(" "))
        ).to(topicSaveTo);
//                .toStream((k,v) -> k.key())
//                .map((k,v) ->
//                        new KeyValue<>(
//                                k, Arrays.stream(
//                                v.split(" "))
//                                .distinct()
//                                .collect(Collectors.joining(" "))
//                        )).to(topicSaveTo);
        KafkaStreams kafkaStreams = new KafkaStreams(myStreamBuilder, properties);
        kafkaStreams.start();
    }
}
