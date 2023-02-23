import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

public class Record {
    /**
     *
     */
    private ArrayList<Object> valuesList;


    /**
     * Constructor of Record
     * @param valuesList the arraylist of values
     */
    public Record(ArrayList<Object> valuesList) {
        this.valuesList = valuesList;

    }

    /**
     * Method gets the list of values
     * @return arraylist Object of values
     */
    public ArrayList<Object> getValuesList() {
        return valuesList;
    }



}
