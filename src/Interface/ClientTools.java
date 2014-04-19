package Interface;

import NetworkInfrastructure.NetworkCommands.Image.CGetServersResponsibleForImageHash;
import NetworkInfrastructure.NetworkCommands.Tag.CGetServersResponsibleForTagHash;
import NetworkInfrastructure.ServerNetworkInfo;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/4/13
 * Time: 9:01 PM
 */
public class ClientTools {

    public class structure implements Comparable {
        public int load;
        public int position;

        public structure(int load, int position) {
            this.load=load;
            this.position=position;
        }

        @Override
        public int compareTo(Object o) {
            return this.load-((structure)o).load;
        }
    }

    public LinkedList<ServerNetworkInfo>
        getServersResponsibleForImageHash(ServerNetworkInfo server, long imageHash) {

        try{
        CGetServersResponsibleForImageHash command=
                new CGetServersResponsibleForImageHash(server,imageHash);
        command.request();
        return command.getServersRequested();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public LinkedList<ServerNetworkInfo>
        getServersResponsibleForTag(ServerNetworkInfo server, long tagHash) {

        try{
            CGetServersResponsibleForTagHash command=
                    new CGetServersResponsibleForTagHash(server,tagHash);
            command.request();
            return command.getServersRequested();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
