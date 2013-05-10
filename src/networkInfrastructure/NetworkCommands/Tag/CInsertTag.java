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
 * Time: 2:25 PM
 */
public class CInsertTag extends NetworkCommand {

    private String tag;
    private long imageValueHash;

    public CInsertTag(ServerNetworkInfo destination, String tag, long imageValueHash) {
        super(destination);
        this.tag = tag;
        this.imageValueHash=imageValueHash;
    }

    @Override
    public String getCode() {
        return "CInsertTag";
    }

    @Override
    public boolean request() throws IOException {
        super.request();

        //we send the Tag
        out.writeUTF(tag);
        out.writeLong(imageValueHash);
        out.flush();

        boolean response=in.readBoolean();
        super.requestClose();
        return response;
    }

    @Override
    public void respond(DataOutputStream out, DataInputStream in,
                        DatabaseSystem databaseSystem) throws IOException {

        String tag=in.readUTF();
        long imageValueHash=in.readLong();
        boolean succeeded = databaseSystem.getLocalDataSystem().
                        insertTag(tag, imageValueHash);
        out.writeBoolean(succeeded);
        out.flush();
    }
}
