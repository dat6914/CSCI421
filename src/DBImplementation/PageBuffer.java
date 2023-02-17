package DBImplementation;
import java.util.ArrayList;

public class PageBuffer {
    private final ArrayList<Page> pagelist;
    int bufferSize;

    public PageBuffer(int bufferSize) {
        this.bufferSize = bufferSize;
        this.pagelist = new ArrayList<>();
    }

    public Page getPage(int pageID) {
        for (Page page : pagelist) {
            if (page.getPageID() == pageID) {
                return page;
            }
        }
        return null;
    }

    //TODO
    //method to write to disk upon quit
    //method to check least recently used page and see if its been modified (compare metadata time block

    public void addPage(Page page) {
        if (pagelist.size() < bufferSize) {
            pagelist.add(page);
        } else {
            //TODO
            // write least recently used page to disk
        }
    }

    public void removePage(Page page) {
        pagelist.remove(page);
    }



}


