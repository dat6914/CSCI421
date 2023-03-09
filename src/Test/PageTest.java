package Test;

import Main.Page;
import Main.Pointer;
import org.junit.jupiter.api.Test;
import Main.Record;
import java.util.ArrayList;

public class PageTest{
    @Test
    void pageTest(){
        ArrayList<Pointer> pointerArrayList = new ArrayList<Pointer>();
        ArrayList<Record> records = new ArrayList<Record>();
        Pointer pointer = new Pointer(500,50);
        Pointer pointer1 = new Pointer(450,50);
        ArrayList<Object> valueList = new ArrayList<>();
        valueList.add(3);
        valueList.add(3.7);
        valueList.add("hi");
        ArrayList<String> attributeTypeList = new ArrayList<>();
        attributeTypeList.add("3");
        attributeTypeList.add("4");
        attributeTypeList.add("52");
        Record record = new Record(valueList,attributeTypeList);
        int numRec = 1;
        int pageId = 1;
        int pagesize = 4096;

        Page page1 = new Page(numRec,pageId,pointerArrayList,records);
        byte[] pageByte = page1.convertPageToByteArr(page1,numRec,pageId,pointerArrayList,records,pagesize);
        Page page2  = Page.convertByteArrToPage(pageByte);


    }

}
