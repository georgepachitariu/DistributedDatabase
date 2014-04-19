package Data;

import ConsistentHashing.HelpingClasses.HashRange;
import ConsistentHashing.HelpingClasses.HashingFunction;

import java.io.File;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/22/13
 * Time: 1:38 PM
 */
public class LocalDataSystem {

    private LinkedList<LocalTagStorage> tagStorageList; // // virtual tag nodes
    private LinkedList<LocalImageStorage> imageStorageList;  // virtual data nodes
    private HashingFunction hashingFunction;
    private String rootDirectory;

    public LocalDataSystem(String rootDirectory) {
        this(new HashingFunction(), new LinkedList<LocalImageStorage>(),
                new LinkedList<LocalTagStorage>(),rootDirectory);
    }

    public LocalDataSystem(HashingFunction h, String rootDirectory) {
        this(h, new LinkedList<LocalImageStorage>(),
                new LinkedList<LocalTagStorage>(),rootDirectory);
    }

    public LocalDataSystem(HashingFunction hashingFunction,
                           LinkedList<LocalImageStorage> imageStorageList,
                           LinkedList<LocalTagStorage> tagStorageList,
                           String rootDirectory ) {
        this.hashingFunction = hashingFunction;
        this.tagStorageList = tagStorageList;
        this.imageStorageList=imageStorageList ;
        this.rootDirectory=rootDirectory;

        File f=new File(rootDirectory);
        if(! f.exists()) f.mkdirs();
    }

    public void addDataVirtualNode(HashRange hr) {
        LocalImageStorage str = new LocalImageStorage(
                this.rootDirectory+String.valueOf(hr.startPoint),   hr  );
        this.imageStorageList.add(str);
    }

    public void addTagVirtualNode(HashRange hr) {
        LocalTagStorage str = new LocalTagStorage( hr  );
        this.tagStorageList.add(str);
    }

    public boolean insertImage(ImageWithMetadata imageStructure) {
        long imageHash=this.hashingFunction.
                getImageHash(imageStructure);

        // we put the structure in the ImageStorage
        // the name of the structure will be the HashValue of the Image
        String filename=String.valueOf(imageHash);

        for(LocalImageStorage storage : this.imageStorageList)
            if(storage.containsInRange(imageHash))
                return storage.insert(filename,imageStructure);
        return false;
    }

    public boolean insertTag(String tag, long imageValueHash) {
        long tagHash=this.hashingFunction.getTagHash(tag);
        return this.insertTag(tagHash,imageValueHash);
    }

    public boolean insertTag(long tagHash, long imageValueHash) {

        for (LocalTagStorage st : this.tagStorageList) {
            if(st.containsInRange(tagHash))
                return st.insert(tagHash, imageValueHash);
        }
        return false;
    }

    public ImageWithMetadata getImage(long imageHash) {
        for(LocalImageStorage storage : this.imageStorageList)
            if(storage.containsInRange(imageHash))
                return storage.getImage(String.valueOf(imageHash));
        return null;
    }

    public LinkedList<Long> getImageHashes(String tag) {
        long tagHash = this.hashingFunction.getTagHash(tag);
        return this.getImageHashes(tagHash);
    }

    public LinkedList<Long> getImageHashes(long tagHash) {
        for (LocalTagStorage st : this.tagStorageList) {
            if(st.containsInRange(tagHash))
                return st.getImageValueHashCollection(tagHash);
        }
        return null;
    }

    public LinkedList<String> deleteImage(long imageHash) {
        for (LocalImageStorage st : this.imageStorageList) {
            if(st.containsInRange(imageHash))
                return st.deleteImage(String.valueOf(imageHash));
        }
        return null;
    }

    public boolean deleteTag(long tagHash, long imageHash) {
        for (LocalTagStorage st : this.tagStorageList) {
            if(st.containsInRange(tagHash))
                return st.delete(tagHash,imageHash);
        }
        return false;
    }

    public LinkedList<ImageWithMetadata> getAllImagesInRange(HashRange range) {
        for(LocalImageStorage storage : this.imageStorageList)
            if( storage.containsInRange(range.startPoint) &&
                    storage.containsInRange(range.endPoint-1) )
                return storage.getAllImagesInRange(range);
        return null;
    }


    public LinkedList<Long> getAllTagsInRange(HashRange range) {
        for(LocalTagStorage storage : this.tagStorageList)
            if(storage.containsInRange(range.startPoint) &&
                    storage.containsInRange(range.endPoint-1))
                return storage.getAllTagKeysInRange(range);
        return null;
    }

    public LinkedList<HashRange> getTagRanges() {
        LinkedList<HashRange> returnList=new LinkedList<HashRange>();
        for(LocalTagStorage st : this.tagStorageList)
            returnList.add(st.getRange());
        return returnList;
    }

    public LinkedList<HashRange> getDataRanges() {
        LinkedList<HashRange> returnList=new LinkedList<HashRange>();
        for(LocalImageStorage st : this.imageStorageList)
            returnList.add(st.getRange());
        return returnList;
    }

    public void deleteAllImagesInRange(HashRange h) {
        for(LocalImageStorage storage : this.imageStorageList)
            if( storage.containsInRange(h.startPoint) &&
                    storage.containsInRange(h.endPoint-1) )
                storage.deleteAllImagesInRange(h);
    }

    public void deleteAllITagsInRange(HashRange h) {
        for(LocalTagStorage storage : this.tagStorageList)
            if( storage.containsInRange(h.startPoint) &&
                    storage.containsInRange(h.endPoint-1) )
                storage.deleteAllTagsInRange(h);
    }

    public void setHashingFunction(HashingFunction hashingFunction) {
        this.hashingFunction = hashingFunction;
    }

    public void moveDataFromTo(HashRange existingNodeSegment, HashRange newNodeSegment) {
        // we get the images from the old segment
        LinkedList<ImageWithMetadata> list=null;
        for(LocalImageStorage storage : this.imageStorageList)
            if( storage.containsInRange(existingNodeSegment.startPoint) &&
                    storage.containsInRange(existingNodeSegment.endPoint-1) )
                list= storage.getAllImagesInRange(newNodeSegment);

       // if(list !=null && list.size() !=0) { // only if there are images
        // we insert them in the new one
        for(LocalImageStorage s:   this.imageStorageList) {
            if(s.getRange().equals( newNodeSegment))
                for(ImageWithMetadata im : list){
                    long imageHash=this.hashingFunction.getImageHash(im);
                    String filename=String.valueOf(imageHash);
                    s.insert(filename, im);
                }
        }

        // and we delete the images from the old segment
        for(LocalImageStorage storage : this.imageStorageList)
            if( storage.containsInRange(existingNodeSegment.startPoint) &&
                    storage.containsInRange(existingNodeSegment.endPoint-1) ) {
                for(ImageWithMetadata im : list)
                    storage.deleteAllImagesInRange(newNodeSegment);
            }

    }

    public void moveTagsFromTo(HashRange existingNodeSegment, HashRange newNodeSegment) {
        // we get the images from the old segment
        LinkedList<Long> keyList=null;
        LocalTagStorage storageWithOldSegment=null;
        for(LocalTagStorage storage : this.tagStorageList)
            if( storage.containsInRange(existingNodeSegment.startPoint) &&
                    storage.containsInRange(existingNodeSegment.endPoint-1) ) {
                storageWithOldSegment=storage;
                keyList= storage.getAllTagKeysInRange(newNodeSegment);
            }

        // we insert them in the new one
        for(LocalTagStorage s : this.tagStorageList) {
            if(s.getRange().equals( newNodeSegment))
                for(Long key : keyList){
                    LinkedList<Long> imageHashes =
                            storageWithOldSegment.getImageValueHashCollection(key);
                    for(Long im : imageHashes)
                        s.insert(key,im);
                }
        }

        // and we delete the images from the old segment
        for(Long key : keyList)
            storageWithOldSegment.deleteAllTagsInRange(newNodeSegment);
    }

    public void resizeOldSegmentToNewOne(
            HashRange existingNodeSegment,
            HashRange newNodeSegment, boolean isDataNode) {
        if(isDataNode)
            for(LocalImageStorage storage : this.imageStorageList)
            {   if(storage.getRange().equals(existingNodeSegment))
                            storage.setRange(newNodeSegment);
            }
        else
            for(LocalTagStorage storage : this.tagStorageList)
                if(storage.getRange().equals(existingNodeSegment)) {
                    storage.setRange(newNodeSegment);
                }
    }

    public String getRootDirectory() {
        return rootDirectory;
    }

}
