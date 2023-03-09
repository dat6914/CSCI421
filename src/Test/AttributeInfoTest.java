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
        System.out.println(a.getType());
        System.out.println(b.getType());
        System.out.println(a.getLength());
        System.out.println(b.getLength());
        assertEquals(a, b);



    }

}
