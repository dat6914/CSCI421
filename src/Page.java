import java.util.ArrayList;

public class Page {
    private int pageID;
    private ArrayList<Record> record_list = new ArrayList<>();

    public Page(int pageID) {
        this.pageID = pageID;
    }

    public ArrayList<Record> getRecord_list() {
        return record_list;
    }

    //delete a record, insert record, get record
    //read page from disk and return Page Object




}
