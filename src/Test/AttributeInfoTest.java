package Test;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import Main.AttributeInfo;

public class AttributeInfoTest {

    @Test
    void testSerialize(){
        AttributeInfo a = new AttributeInfo("3", 4);
        byte[] byteArr = a.serializeAttributeInfo();
        AttributeInfo b = a.deserializeAttributeInfo(byteArr);
        assertEquals(a, b);



    }

}
