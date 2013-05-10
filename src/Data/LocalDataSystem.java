package Data;

import ConsistentHashing.HashRange;
import ConsistentHashing.HashingFunction;

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

    public LocalDataSystem() {
        this(new HashingFunction(), new LinkedList<LocalImageStorage>(),
                 new LinkedList<LocalTagStorage>(),"");
    }

    public LocalDataSystem(String rootDirectory, HashingFunction hashingFunction) {
        this(   hashingFunction,
                    new LinkedList<LocalImageStorage>(),
                    new LinkedList<LocalTagStorage>(),
                    rootDirectory   );
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
}
