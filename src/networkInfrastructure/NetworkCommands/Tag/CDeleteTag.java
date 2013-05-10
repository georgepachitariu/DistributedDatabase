package networkInfrastructure.NetworkCommands.Tag;

import ConsistentHashing.HashingFunction;
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
 * Time: 2:50 PM
 */
public class CDeleteTag extends NetworkCommand  {
        private String tag;
        private long imageValueHash;

        public CDeleteTag(ServerNetworkInfo destination, String tag, long imageValueHash) {
            super(destination);
            this.imageValueHash=imageValueHash;
            this.tag=tag;
    }

        @Override
        public String getCode() {
        return "CDeleteTag";
    }

        @Override
        public boolean request() throws IOException {
        super.request();


        long tagHash = new HashingFunction().getTagHash(tag);
        //we send the Tag
        out.writeLong(tagHash);
        out.writeLong(imageValueHash);
        out.flush();

        boolean response=in.readBoolean();
        super.requestClose();
        return response;
    }

        @Override
        public void respond(DataOutputStream out, DataInputStream in,
            DatabaseSystem databaseSystem) throws IOException {

        long tagHash=in.readLong();
        long imageValueHash=in.readLong();
        boolean succeeded = databaseSystem.getLocalDataSystem().
                deleteTag(tagHash, imageValueHash);
        if(succeeded)
            out.writeBoolean(true);
        else
            out.writeBoolean(false);
       out.flush();
    }
    }
