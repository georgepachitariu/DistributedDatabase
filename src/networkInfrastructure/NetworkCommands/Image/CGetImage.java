package networkInfrastructure.NetworkCommands.Image;

import Data.DatabaseSystem;
import Data.ImageWithMetadata;
import networkInfrastructure.NetworkCommands.NetworkCommand;
import networkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/4/13
 * Time: 1:26 PM
 */
public class CGetImage extends NetworkCommand {

    // request members
    private long imageHash;
    private ImageWithMetadata imageWithMetadata;

    public CGetImage(ServerNetworkInfo server, long imageHash) {
        super(server);
        this.imageHash=imageHash;
    }

    @Override
    public String getCode() {
        return "CGetImage";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        //we send the ImageHash
        out.writeLong(imageHash);
        out.flush();

        //we read the Image
        int expectedLength=in.readInt();

        byte[] raw=super.getAllAvailableBytesFromStream(in, expectedLength);
        if(raw ==null)
            return false;
        this.imageWithMetadata=new ImageWithMetadata(raw);

        super.requestClose();
        return true;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        long hash=in.readLong();
        ImageWithMetadata im = databaseSystem.getLocalDataSystem().getImage(hash);

        // we send the image to the requester.
        byte[] imageRaw = im.toBytes();
        out.writeInt(imageRaw.length);
        out.write(imageRaw);
        out.flush();
    }


    public ImageWithMetadata getImageWithMetadata() {
        return imageWithMetadata;
    }
}
