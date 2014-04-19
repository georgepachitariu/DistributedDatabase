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
 * Time: 11:30 PM
 */
public class CGetServersResponsibleForTagHash extends NetworkCommand {

    //request members
    private long tagHash;
    // response members
    private LinkedList<ServerNetworkInfo> serversRequested;

    public CGetServersResponsibleForTagHash(
            ServerNetworkInfo server, long tagHash ) {
        super(server);
        this.tagHash=tagHash;
        this.serversRequested=new LinkedList<ServerNetworkInfo>();
    }

    @Override
    public String getCode() {
        return "CGetServersResponsibleForTagHash";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        //we send the TagHash
        out.writeLong(tagHash);
        out.flush();

        //we read the Addresses
        int listNr=in.readInt();
        for(int i=0; i< listNr; i++) {
            String response=super.in.readUTF();
            this.serversRequested.add(new ServerNetworkInfo(response));
            out.writeBoolean(true);
            out.flush();
        }

        super.requestClose();
        return true;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        long tagHash=in.readLong();
        LinkedList<ServerNetworkInfo> servers=databaseSystem.getDataDistributionManager().
                getServersResponsibleForTagHash(tagHash);

        // we send the image to the requester
        out.writeInt(servers.size());
        for(ServerNetworkInfo s : servers) {
            out.writeUTF(s.toString());
            out.flush();
            if(! in.readBoolean()) return;
        }
    }

    public LinkedList<ServerNetworkInfo> getServersRequested() {
        return this.serversRequested;
    }
}