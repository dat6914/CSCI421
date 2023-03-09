package Main;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class AttributeInfo {
    private String type;
    private int length;

    public AttributeInfo(String type, int length){
        this.type = type;
        this.length = length;
    }

    public String getType(){
        return this.type;
    }

    public int getLength(){
        return this.length;
    }

    public byte[] serializeAttributeInfo(){
        //byte[] strArr = ((String) temp).getBytes(StandardCharsets.UTF_8)

        //byte[] typeByteArray = ByteBuffer.allocate(4).putChar(this.type).array();

        //byte[] typeByteArray = (this.type).getBytes(StandardCharsets.UTF_8);

        byte[] typeByteArray = (this.type).getBytes(StandardCharsets.UTF_8);


        byte[] lengthByteArray = ByteBuffer.allocate(Integer.BYTES).putInt(this.length).array();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
        try {
            outputStream.write(typeByteArray);
            outputStream.write(lengthByteArray);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        byte[] attributeInfoByteArray = outputStream.toByteArray( );

        return attributeInfoByteArray;
    }

    public static AttributeInfo deserializeAttributeInfo(byte[] byteArray){
        int idx = 0;


        byte[] typeByteArray = Arrays.copyOfRange(byteArray, idx, idx + 1);
        String type = new String(typeByteArray,StandardCharsets.UTF_8);
        //char type = ByteBuffer.wrap(typeByteArray).getChar();
        idx += 1;
        byte[] lengthByteArray = Arrays.copyOfRange(byteArray, idx, idx + Integer.BYTES);
        int length = ByteBuffer.wrap(lengthByteArray).getInt();
        AttributeInfo a = new AttributeInfo(type, length);
        return a;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AttributeInfo that = (AttributeInfo) o;
        return type.equals(that.type) && length == that.length;
    }

    @Override
    public String toString(){
        return "AttributeInfo{type = " + type + " , length = " + length + "}";
    }

}
