package Data;

import ConsistentHashing.HelpingClasses.HashRange;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/22/13
 * Time: 11:33 AM
 */
public class LocalTagStorage {

    private HashMap<Long, LinkedList<Long>> storage;
    private HashRange range;

    public HashRange getRange() {
        return range;
    }

    public boolean containsInRange(long imageHash) {
        return this.range.contains(imageHash);
    }

    public LocalTagStorage(HashRange range) {
        this.storage = new HashMap<Long, LinkedList<Long>>(1000);
        this.range=range;
    }

    public boolean insert(long tagHash,long imageValueHash) {
        LinkedList<Long> list=this.storage.get(tagHash);
        if(list == null)
            list=new LinkedList<Long>();
        boolean returnValue = list.add(imageValueHash);

        this.storage.put(tagHash, list);
        return returnValue;
    }

    public LinkedList<Long> getImageValueHashCollection(long tagHash) {
        return this.storage.get(tagHash);
    }

    public boolean delete(long tagHash,long imageValueHash) {
        LinkedList<Long> list=this.storage.get(tagHash);

        boolean deleted = list.remove(imageValueHash);
        this.storage.put(tagHash,list);

        if(deleted) return true;
        else    return false;
    }

    public LinkedList<Long> getAllTagKeysInRange(HashRange range) {
        LinkedList<Long> returnList=new LinkedList<Long>();
        Set<Long> keySet = this.storage.keySet();
        for(Long key : keySet)
            if(range.contains(key))
                returnList.add(key);
        return  returnList;
    }

    public void deleteAllTagsInRange(HashRange h) {
        LinkedList<Long> list = this.getAllTagKeysInRange(h);
        for(Long key : list) {
            // by overwriting with null, we delete the previous data
            this.storage.put(key,null);
        }
    }

    public void setRange(HashRange range) {
        this.range = range;
    }
}
