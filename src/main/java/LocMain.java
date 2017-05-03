import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Created by DmitriyBrosalin on 27/04/2017.
 */
public class LocMain {

    public static void main(String[] args){
        SimpleMessage simpleMessage = new SimpleMessage();
        simpleMessage.host = "ya.ru";
        simpleMessage.imsi = "asd13";
        simpleMessage.path = "1234";
        Gson gson = new GsonBuilder().create();
        String res = gson.toJson(simpleMessage);
        System.out.println(res);
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(res);
        SimpleMessage simpleMessage1 = gson.fromJson(element, SimpleMessage.class);
        System.out.println(simpleMessage1.host);
    }

}
