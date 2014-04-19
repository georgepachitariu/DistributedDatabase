package NetworkInfrastructure.NetworkCommands;

import Data.DatabaseSystem;
import NetworkInfrastructure.ServerNetworkInfo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/27/13
 * Time: 11:42 AM
 */
public class CPing  extends NetworkCommand {

    public CPing(ServerNetworkInfo server) {
        super(server);
    }

    @Override
    public String getCode() {
        return "CPing";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        out.writeBoolean(true);
        out.flush();
        boolean response=in.readBoolean();

        super.requestClose();
        return response;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {
        out.writeBoolean(in.readBoolean());
        out.flush();
    }

}
