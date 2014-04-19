package NetworkInfrastructure.NetworkCommands;

import Data.DatabaseSystem;
import NetworkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/7/13
 * Time: 10:14 PM
 */
public class CCloseThread extends NetworkCommand
{

    public CCloseThread(ServerNetworkInfo destination ) {
            super(destination);
    }

    @Override
    public String getCode() {
        return "CCloseThread";
    }

    @Override
    public boolean request() throws IOException {
        super.request();
        boolean response=in.readBoolean();
        super.requestClose();
        return response;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        databaseSystem.getIncomingConnectionsThread().setAlive(false);
        out.writeBoolean(true);
        out.flush();
    }
}
