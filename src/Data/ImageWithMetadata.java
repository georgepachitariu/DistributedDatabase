package Data;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/21/13
 * Time: 1:59 PM
 */
public class ImageWithMetadata {
    public String fileName;  /*imageValueHash (it contains the extension)*/
    public byte[]  raw;
    public LinkedList<String> tags;

    public ImageWithMetadata(String fileName, byte[] raw, LinkedList<String> tags) {
        this.fileName = fileName;
        this.raw = raw;
        this.tags = tags;
    }

    public ImageWithMetadata() {
        this.tags=new LinkedList<String>();
        this.fileName=new String();
        this.raw=new byte[0];
    }

    public ImageWithMetadata(byte[] raw) {
        this.createObjectFromRaw(raw);
    }

    public ImageWithMetadata(Path location) {
        try {
            byte[] bytes = Files.readAllBytes(location);
            this.createObjectFromRaw(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

/*    public ImageWithMetadata(String rawImg) {
        byte[]img =rawImg.getBytes();
        this.createObjectFromRaw(img);
    }*/

    @Override
    public boolean equals(Object o) {
        if(Arrays.equals(this.toBytes(),((ImageWithMetadata)o).toBytes()))
            return true;
        else return false;
    }

    @Override
    public String toString(){
        return null;
    }

    private void createObjectFromRaw(byte[] raw) {
        int i;

        //We parse the FILE NAME
        for( i=0;   raw[i]!="\t".getBytes()[0];  i++) ;
        this.fileName=new String( Arrays.copyOf(raw, i));
        i++; // we pass the "\n" character
        int k=i; //last index

        //We parse the TAGS
        this.tags=new LinkedList<String>();
        for( ; raw[i]!="\t".getBytes()[0];  i++) ;
        String c=new String( Arrays.copyOfRange(raw, k, i));
        int tagsCount=Integer.parseInt(c);
        i++; // we pass the "\n" character
        for(int j=0;j<tagsCount;j++) {
            k=i; //last index
            for( ; raw[i]!="\t".getBytes()[0];  i++) ;
            String tag=new String( Arrays.copyOfRange(raw, k, i));
            this.tags.add(tag);
            i++; // we pass the "\n" character
        }

        //We parse the IMAGE
        this.raw = Arrays.copyOfRange(raw, i, raw.length);
    }

    public byte[] toBytes() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
        byte[] returnBytes=null;
        try {
            outputStream.write(this.fileName.getBytes());
            outputStream.write("\t".getBytes());

            outputStream.write(String.valueOf(this.tags.size()).getBytes());
            outputStream.write("\t".getBytes());
            for(String tag : this.tags) {
                outputStream.write(tag.getBytes());
                outputStream.write("\t".getBytes());
            }
            outputStream.write(this.raw);

            returnBytes = outputStream.toByteArray();
            outputStream.close();
        } catch (IOException e) {       }

        return returnBytes;
    }

    // location+filename
    public boolean writeToDisk(String location) {
        File f = new File(location);
        try {
            if (! f.createNewFile() )
                return false;
            FileOutputStream fos = new FileOutputStream( location );
            fos.write(this.toBytes());
            fos.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
