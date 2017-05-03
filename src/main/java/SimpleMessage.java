/**
 * Created by DmitriyBrosalin on 27/04/2017.
 */
public class SimpleMessage {
    public String host;
    public String path;
    public String imsi;

    @Override
    public String toString(){
        return host + " " + path + " " + imsi;
    }
}
