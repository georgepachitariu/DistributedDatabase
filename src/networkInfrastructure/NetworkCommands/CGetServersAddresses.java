package networkInfrastructure.NetworkCommands;

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
 * Date: 5/5/13
 * Time: 12:02 AM
 */
public class CGetServersAddresses extends NetworkCommand {

    // request members
    private ImageWithMetadata imageWithMetadata;
    // response members
    private LinkedList<ServerNetworkInfo> serversRequested;

    public CGetServersAddresses(ServerNetworkInfo server) {
        super(server);
        this.serversRequested=new LinkedList<ServerNetworkInfo>();
    }

    @Override
    public String getCode() {
        return "CGetServersAddresses";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        //we read the Addresses
        int length=in.readInt(); // list length
        for(int i=0; i<length; i++) {
            String response=super.in.readUTF();
            this.serversRequested.add(new ServerNetworkInfo(response));
        }

        super.requestClose();
        return true;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        LinkedList<ServerNetworkInfo> list = databaseSystem.
                getServerDistributionManager().getServersAddresses();

        // we send the image to the requester
        out.writeInt(list.size());  // list length
        for(ServerNetworkInfo el : list)
            out.writeUTF(el.toString());
        out.flush();
    }

    public LinkedList<ServerNetworkInfo> getServersRequested() {
        return this.serversRequested;
    }
}