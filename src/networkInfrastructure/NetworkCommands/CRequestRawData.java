package networkInfrastructure.NetworkCommands;

import ConsistentHashing.HashRange;
import Data.DatabaseSystem;
import Data.ImageWithMetadata;
import networkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/28/13
 * Time: 3:50 PM
 */
public class CRequestRawData extends NetworkCommand {

    private HashRange dataRange;
    private final DatabaseSystem requester;
    private ServerNetworkInfo source;

    public CRequestRawData(HashRange dataRange,ServerNetworkInfo source,
                           DatabaseSystem requestServer) {
        super(source);
        this.source=source;
        this.dataRange=dataRange;
        this.requester=requestServer;
    }

    @Override
    public String getCode() {
        return "CRequestRawData";
    }

    @Override
    public boolean request() throws IOException {
        super.request();
        //we add the hashRange for which we want the raw data
        out.writeUTF(dataRange.toString());
        out.flush();

        int nr=in.readInt();
        for(int i=0; i<nr; i++) {
            int expectedLength = in.readInt();
            byte[] raw=super.getAllAvailableBytesFromStream(in,expectedLength );
            ImageWithMetadata image=new ImageWithMetadata(raw);
            boolean succeeded=requester.getLocalDataSystem().
                    insertImage(image);
            if(succeeded == false)
                return false;

            out.writeBoolean(true);  //acceptance byte
            out.flush();
        }

        super.requestClose();
        return true;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {

        HashRange range=new HashRange(in.readUTF());
        LinkedList<ImageWithMetadata> imagesList =
                databaseSystem.getLocalDataSystem().getAllImagesInRange(range);

        //we send the number of images
        out.writeInt(imagesList.size());

        // we serialise and send the images
        for(ImageWithMetadata img : imagesList) {
            byte[] imageRaw = img.toBytes();
            out.writeInt(imageRaw.length);
            out.write(imageRaw);
            out.flush();
            // we read the acceptance byte
            if(! in.readBoolean())
                break;
        }
    }
}
