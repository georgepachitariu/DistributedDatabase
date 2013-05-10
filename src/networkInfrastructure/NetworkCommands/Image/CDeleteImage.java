package networkInfrastructure.NetworkCommands.Image;

import Data.DatabaseSystem;
import Data.ImageWithMetadata;
import networkInfrastructure.NetworkCommands.NetworkCommand;
import networkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/4/13
 * Time: 2:00 PM
 */
public class CDeleteImage extends NetworkCommand {

    // request members
    private long imageHash;
    private ImageWithMetadata imageWithMetadata;

    //response members
    private LinkedList<String> tags;

    public CDeleteImage(ServerNetworkInfo server, long imageHash) {
        super(server);
        this.imageHash=imageHash;
        this.tags=new LinkedList<String>();
    }

    @Override
    public String getCode() {
        return "CDeleteImage";
    }

    @Override
    public boolean request() throws IOException {
        super.request();
        //we send the ImageHash
        out.writeLong(imageHash);
        out.flush();

        //we read the tags associated with the image
        int size=in.readInt(); //list size
        for(int i=0; i < size; i++) {
            String tag=super.in.readUTF();
            this.tags.add(tag);
        }
        if(this.tags.size()==0)
            return false;

        super.requestClose();
        return true;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        long hash=in.readLong();
        LinkedList<String> list=
            databaseSystem.getLocalDataSystem().deleteImage(hash);

        // we send the associated tags to the requester
        out.writeInt(list.size()); //list size
        for(String el : list)
            out.writeUTF(el);
        out.flush();
    }

    public LinkedList<String> getTags() {
        return this.tags;
    }
}