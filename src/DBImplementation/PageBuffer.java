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


}


