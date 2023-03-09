package Test;

import Main.Pointer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PointerTest {
    @Test
    public void test(){
        Pointer p = new Pointer(3,4);
        byte[] byteArr = p.serializePointer();
        Pointer p2 = Pointer.deserializePointer(byteArr);
        assertEquals(p,p2);
    }
}
