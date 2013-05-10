package networkInfrastructure.NetworkCommands.Image;

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
 * Time: 9:19 PM
 */
public class CGetServersResponsibleForImageHash extends NetworkCommand {

    //request members
    private long imageHash;
    // response members
    private ServerNetworkInfo serverRequested;

    public CGetServersResponsibleForImageHash(
            ServerNetworkInfo server, long imageHash ) {
        super(server);
        this.imageHash=imageHash;
    }

    @Override
    public String getCode() {
        return "CGetServersResponsibleForImageHash";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        //we send the ImageHash
        out.writeLong(imageHash);
        out.flush();

        //we read the Address
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
                getServersResponsibleForImageHash(imageHash);

        // we send the address to the requester
        out.writeUTF(server.toString());
        out.flush();
    }

    public ServerNetworkInfo getServerRequested() {
        return this.serverRequested;
    }
}