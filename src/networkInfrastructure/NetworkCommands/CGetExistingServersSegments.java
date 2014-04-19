package NetworkInfrastructure.NetworkCommands;

import ConsistentHashing.DistributionManager;
import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import Data.DatabaseSystem;
import NetworkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/22/13
 * Time: 6:26 PM
 */
public class CGetExistingServersSegments extends NetworkCommand {

    // response members
    private LinkedList<ServerSegmentsStruct> serversRequested;
    private  int imageDuplicationNr;
    private  int tagDuplicationNr;

    public CGetExistingServersSegments(ServerNetworkInfo server) {
        super(server);
        this.serversRequested=new LinkedList<ServerSegmentsStruct>();
    }

    @Override
    public String getCode() {
        return "CGetExistingServersSegments";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        this.imageDuplicationNr=in.readInt();
        this.tagDuplicationNr=in.readInt();

        //we read the Addresses
        int length=in.readInt(); // list length
        for(int i=0; i<length; i++) {
            String response=super.in.readUTF();
            this.serversRequested.add(new ServerSegmentsStruct(response));
        }

        super.requestClose();
        return true;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        DistributionManager distributionManager = databaseSystem.getDistributionManager();
        LinkedList<ServerSegmentsStruct> list =distributionManager .getExistingServers();

        out.writeInt(distributionManager.getImageDuplicationNr());
        out.writeInt(distributionManager.getTagDuplicationNr());

        // we send the image to the requester
        out.writeInt(list.size());  // list length
        for(ServerSegmentsStruct el : list)
            out.writeUTF(el.toString());
        out.flush();
    }

    public LinkedList<ServerSegmentsStruct> getServersRequested() {
        return this.serversRequested;
    }

    public int getImageDuplicationNr() {
        return imageDuplicationNr;
    }

    public int getTagDuplicationNr() {
        return tagDuplicationNr;
    }

}
