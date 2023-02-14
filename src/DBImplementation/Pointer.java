package DBImplementation;

public class Pointer {
    private int offset;

    private int length;

    public Pointer(int offset, int length){
        this.offset = offset;
        this.length = length;
    }

    public int getOffset(){
        return offset;
    }

    public int getLength() {
        return length;
    }

    public void updateOffset(int val){
        offset+=val;
    }


}
