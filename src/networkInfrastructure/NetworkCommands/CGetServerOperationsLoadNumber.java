package networkInfrastructure.NetworkCommands;

import Data.DatabaseSystem;
import networkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/4/13
 * Time: 11:32 AM
 */
public class CGetServerOperationsLoadNumber extends NetworkCommand {

    private int loadNumber;

    public CGetServerOperationsLoadNumber(ServerNetworkInfo server) {
        super(server);
    }

    @Override
    public String getCode() {
        return "CGetServerOperationsLoadNumber";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        //we read the load number
        this.loadNumber=in.readInt();

        super.requestClose();
        return true;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        int loadNumber=databaseSystem.getIncomingConnectionsThread().
                        getCommandsQueueLoadNumber();
        out.writeInt(loadNumber);
        out.flush();
    }

    public int getLoadNumber() {
        return loadNumber;
    }
}
