import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * CSCI420 Project - Phase 1
 * Group 3
 */

/**
 * This class is to store record's values
 */
public class Record {

    private ArrayList<Object> valuesList;

    private ArrayList<Pointer> pointers = new ArrayList<>();

    /**
     * Constructor of Record
     * @param valuesList the arraylist of values
     */
    public Record(ArrayList<Object> valuesList, ArrayList<Pointer> pointers) {
        this.valuesList = valuesList;
        this.pointers = pointers;

    }

    /**
     * Method gets the list of values
     * @return arraylist Object of values
     */
    public ArrayList<Object> getValuesList() {
        return valuesList;
    }

}
