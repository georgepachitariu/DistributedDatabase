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
 * Date: 5/2/13
 * Time: 9:13 AM
 */
public class CInsertImage extends NetworkCommand {

    private ImageWithMetadata image;

    public CInsertImage(ServerNetworkInfo destination,ImageWithMetadata image ) {
        super(destination);
        this.image = image;
    }

    @Override
    public String getCode() {
        return "CInsertImage";
    }

    @Override
    public boolean request() throws IOException {
       super.request();
        //we send the Image
        byte[] imageRaw = image.toBytes();
        out.writeInt(imageRaw.length);
        out.write(imageRaw);
        out.flush();

        boolean response=in.readBoolean();
        super.requestClose();
        return response;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        int expectedLength=in.readInt();
        byte[] raw=super.getAllAvailableBytesFromStream(in,expectedLength);
        ImageWithMetadata im=new ImageWithMetadata(raw);

        boolean succeeded = databaseSystem.getLocalDataSystem().insertImage(im);
        out.writeBoolean(succeeded);
        out.flush();
    }
}
