package Interface;

import networkInfrastructure.NetworkCommands.CGetServerOperationsLoadNumber;
import networkInfrastructure.NetworkCommands.Image.CGetServersResponsibleForImageHash;
import networkInfrastructure.NetworkCommands.Tag.CGetServersResponsibleForTagHash;
import networkInfrastructure.ServerNetworkInfo;

import java.io.IOException;
import java.util.Collections;
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

    public ServerNetworkInfo getFreeServerFrom(LinkedList<ServerNetworkInfo> list) {

        LinkedList<structure>listLoadNumbers=new LinkedList<structure>();

        for(int i=0; i<list.size(); i++) {
            int load=getServerLoad(list.get(i));
            listLoadNumbers.add(new structure(load, i));
        }

        Collections.sort(listLoadNumbers);
        return list.get( listLoadNumbers.getFirst().position );
    }

    public int getServerLoad(ServerNetworkInfo serverNetworkInfo) {

        try {
            CGetServerOperationsLoadNumber command=
                    new CGetServerOperationsLoadNumber(serverNetworkInfo);
            command.request();
            return command.getLoadNumber();
        } catch (IOException e) {        }
        return -1;
    }

    public ServerNetworkInfo
        getServerResponsibleForImageHash(ServerNetworkInfo server, long imageHash) {

        try{
        CGetServersResponsibleForImageHash command=
                new CGetServersResponsibleForImageHash(server,imageHash);
        command.request();
        return command.getServerRequested();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ServerNetworkInfo
        getServerResponsibleForTag(ServerNetworkInfo server, long tagHash) {

        try{
            CGetServersResponsibleForTagHash command=
                    new CGetServersResponsibleForTagHash(server,tagHash);
            command.request();
            return command.getServerRequested();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
