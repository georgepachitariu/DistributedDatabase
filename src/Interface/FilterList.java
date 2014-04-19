package Interface;

import java.util.Hashtable;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/26/13
 * Time: 12:50 PM
 */
public class FilterList {

    public LinkedList<Long> filterList(LinkedList<Long> allHashesList, int nrOfOccurencesRequired) {
        Hashtable<Long,Byte> dictionary=new Hashtable<Long, Byte>();
        LinkedList<Long> filteredImageList=new LinkedList<Long>();

        for(Long hash : allHashesList) {
            Byte val=dictionary.get(hash);
            if(val == null)
                val=new Byte((byte)0);
            val++;
            dictionary.put(hash,val);
            if(val==nrOfOccurencesRequired)
                filteredImageList.add(hash);
        }

        return  filteredImageList;
    }
}
