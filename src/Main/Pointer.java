package Main;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;


public class Pointer {
    private int offset;
    private int length;

    public Pointer(int offset, int length){
        this.offset = offset;
        this.length = length;
    }

    public int getOffset(){
        return this.offset;
    }

    public int getLength(){
        return this.length;
    }

    /**
     * Serialize pointer object into byte array
     * @return byte[] representation of pointer
     */
    public byte[] serializePointer(){
        byte[] offsetByteArray = ByteBuffer.allocate(Integer.BYTES).putInt(this.offset).array();
        byte[] lengthByteArray = ByteBuffer.allocate(Integer.BYTES).putInt(this.length).array();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
        try {
            outputStream.write(offsetByteArray);
            outputStream.write(lengthByteArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] pointerByteArray = outputStream.toByteArray( );
        return pointerByteArray;
    }

    /**
     * Deserialize byte array into pointer object
     * @param pointerByteArray
     * @return Main.Pointer object
     */
    public static Pointer deserializePointer(byte[] pointerByteArray){
        int idx = 0;
        byte[] offsetByteArray = Arrays.copyOfRange(pointerByteArray, idx, idx + Integer.BYTES);
        int offset = ByteBuffer.wrap(offsetByteArray).getInt();
        idx += Integer.BYTES;
        byte[] lengthByteArray = Arrays.copyOfRange(pointerByteArray, idx, idx + Integer.BYTES);
        int length = ByteBuffer.wrap(lengthByteArray).getInt();
        Pointer p = new Pointer(offset, length);
        return p;
    }

    @Override
    public String toString(){
        return "Offset = " + offset + " length = " + length;
    }

    @Override
    public boolean equals(Object o){
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Pointer that = (Pointer) o;
        return offset == that.offset && length == that.length;
    }
}
