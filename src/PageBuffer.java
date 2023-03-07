import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

public class PageBuffer {
    private String db_loc;
    private int buffer_size;
    private ArrayList<Page> pageList = new ArrayList<>();


    public PageBuffer(String db_loc, int buffer_size, ArrayList<Page> pageList){
        this.buffer_size = buffer_size;
        this.db_loc = db_loc;
        this.pageList = pageList;

    }

    public int getBuffer_size(){
        return this.buffer_size;
    }


    public ArrayList<Page> getPageList(){
        return this.pageList;
    }

    public boolean CheckBufferFull(){
        if (this.pageList.size() > this.buffer_size){
            return true;
        }
        return false;
    }

    /**
     * Method writes the byte array of object to file
     * @param path the path of file
     * @param data byte array of data need to be stored in file
     */
    public void writeByteArrToDisk(String path, byte[] data) {
        File tempFile = new File(path);
        if (tempFile.exists()) {
            try {
                FileOutputStream fs = new FileOutputStream(path);
                fs.write(data);
                fs.close();
            } catch (IOException i) {
                System.err.println("An error occurred while writing to the file.");
                i.printStackTrace();
                System.err.println("ERROR");
            }

        } else {
            try {
                FileOutputStream fs = new FileOutputStream(path);
                fs.write(data);
            } catch (IOException e) {
                System.err.println("An error occurred while writing to the file.");
                e.printStackTrace();
                System.err.println("ERROR");

            }
        }
    }

    public boolean quitProgram(StorageManager storageManager, ArrayList<Page> pageList) {
        Catalog catalog =  storageManager.getCatalog();
        if (catalog != null) {
            byte[] catalogByteArr = catalog.convertCatalogToByteArr(catalog);
            String catalogPath = this.db_loc + "/catalog.txt";
            writeByteArrToDisk(catalogPath, catalogByteArr);
            for (int i = 0; i < pageList.size(); i++) {
                Page pageToWrite = pageList.get(i);
                byte[] byteArr = pageToWrite.convertPageToByteArr(pageToWrite.getRecordList(), pageToWrite.getTable(), pageToWrite.getCurrent_page_size());
                String path = this.db_loc  + "/Pages/" + pageToWrite.getPageID() + ".txt";
                writeByteArrToDisk(path, byteArr);
            }
        } else {
            System.err.println("Fails to write catalog to file!");
            System.err.println("ERROR");
            return false;
        }
        return true;
    }
}
