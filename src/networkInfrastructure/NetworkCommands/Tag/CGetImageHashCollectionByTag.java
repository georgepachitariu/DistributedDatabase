package NetworkInfrastructure.NetworkCommands.Tag;

import Data.DatabaseSystem;
import NetworkInfrastructure.NetworkCommands.NetworkCommand;
import NetworkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/4/13
 * Time: 3:24 PM
 */
public class CGetImageHashCollectionByTag extends NetworkCommand {

    // request members
    private String tag;
    //response members
    private LinkedList<Long> imagesHashList;

    public CGetImageHashCollectionByTag(ServerNetworkInfo server, String tag) {
        super(server);
        this.tag=tag;
        this.imagesHashList=new LinkedList<Long>();
    }

    @Override
    public String getCode() {
        return "CGetImageHashCollectionByTag";
    }

    @Override
    public boolean request() throws IOException {
        super.request();
        //we send the tag
        out.writeUTF(tag);
        out.flush();

        //we read the hashes of images
        int size=in.readInt();//the size of the list
        String hash;
        for(int i=0; i<size; i++) {
            long hashV=super.in.readLong();
            this.imagesHashList.add(hashV);
        }

        super.requestClose();
        return true;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        String hash=in.readUTF();
        LinkedList<Long> list = databaseSystem.getLocalDataSystem().
                getImageHashes(hash);
        if(list==null) list=new LinkedList<Long>();

        // we send the hashes to the requester
        out.writeInt(list.size());  //the size of the list
        for(Long el : list)
            out.writeLong(el);
        out.flush();
    }


    public LinkedList<Long> getImagesHashList() {
        return this.imagesHashList;
    }
}