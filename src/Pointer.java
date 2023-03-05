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
}
