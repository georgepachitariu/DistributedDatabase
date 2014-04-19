package Data;

import ConsistentHashing.HelpingClasses.HashRange;

import java.io.File;
import java.nio.file.Paths;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/18/13
 * Time: 12:48 PM
 */
public class LocalImageStorage {

    private String currentDirectory;
    private HashRange range;

    public LocalImageStorage(String rootDirectory, HashRange range) {
        new File(rootDirectory).mkdir();
        this.currentDirectory = rootDirectory;
        this.range=range;
    }

    public String getCurrentDirectory() {
        return currentDirectory;
    }

    public boolean containsInRange(long imageHash) {
        return this.range.contains(imageHash);
    }

    public boolean insert(String filename, ImageWithMetadata image) {
        if(image.raw == null)
            return false;  // it doesn't make sense to try to insert in an
        // image-detabase a record without an image

        return image.writeToDisk(this.currentDirectory+"/" + filename);
    }

    public ImageWithMetadata  getImage( String fileName ) {
        String location=this.currentDirectory+"/"+fileName;
        if(new File(location).exists())
            return new ImageWithMetadata(Paths.get(location));
        else return null;
    }

    public LinkedList<String> deleteImage(String imageHash) {
        ImageWithMetadata imageToBeDeleted = this.getImage(imageHash);
        if(! new File(this.currentDirectory+"/"+imageHash).delete() )
            return null;
        return imageToBeDeleted.tags;
    }

    public LinkedList<ImageWithMetadata> getAllImagesInRange(HashRange range) {
        File rootFolder = new File(this.currentDirectory);
        LinkedList<ImageWithMetadata> returnList= new LinkedList<ImageWithMetadata>();
        File[] imageList = rootFolder.listFiles();
        for( File f : imageList ) {
            long fileName=Long.valueOf( f.getName());
            if( fileName >= range.startPoint && fileName < range.endPoint)
                returnList.add(this.getImage(f.getName()));
        }
        return returnList;
    }

    public void setRange(HashRange range) {
        this.range = range;
    }

    public HashRange getRange() {
        return range;
    }

    public void deleteAllImagesInRange(HashRange h) {
        LinkedList<ImageWithMetadata> imagesToBeDeleted =
                this.getAllImagesInRange(h);
        for(ImageWithMetadata image: imagesToBeDeleted)
            this.deleteImage(image.fileName);
    }
}
