package networkInfrastructure.NetworkCommands.Tag;

import Data.DatabaseSystem;
import networkInfrastructure.NetworkCommands.NetworkCommand;
import networkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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
    private ServerNetworkInfo serverRequested;

    public CGetServersResponsibleForTagHash(
            ServerNetworkInfo server, long tagHash ) {
        super(server);
        this.tagHash=tagHash;
    }

    @Override
    public String getCode() {
        return "CGetServersResponsibleForTagHash";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        //we send the ImageHash
        out.writeLong(tagHash);
        out.flush();

        //we read the Image
        String response=super.in.readUTF();
        if(response ==null)
            return false;
        this.serverRequested=new ServerNetworkInfo(response);

        super.requestClose();
        return true;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        long imageHash=in.readLong();
        ServerNetworkInfo server=databaseSystem.getDataDistributionManager().
                getServersResponsibleForTagHash(imageHash);

        // we send the image to the requester
        out.writeUTF(server.toString());
        out.flush();
    }

    public ServerNetworkInfo getServerRequested() {
        return this.serverRequested;
    }
}