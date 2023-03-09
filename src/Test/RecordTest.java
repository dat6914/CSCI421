package Test;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import Main.Record;

import java.util.ArrayList;

public class RecordTest {
    @Test
    void testSerialize(){
        ArrayList<Object> valueList = new ArrayList<>();
        valueList.add(3);
        valueList.add(3.7);
        valueList.add("hi");

        ArrayList<String> attributeTypeList = new ArrayList<>();
        attributeTypeList.add("3");
        attributeTypeList.add("4");
        attributeTypeList.add("52");


        Record r1 = new Record(valueList,attributeTypeList);
        byte[] recordByte = r1.convertRecordToByteArr(r1);
        Record r2 = Record.convertByteArrToRecord(recordByte);
        assertEquals(r1,r2);

    }
}
