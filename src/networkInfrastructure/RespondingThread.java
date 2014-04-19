package NetworkInfrastructure;

import Data.DatabaseSystem;
import NetworkInfrastructure.NetworkCommands.*;
import NetworkInfrastructure.NetworkCommands.Image.*;
import NetworkInfrastructure.NetworkCommands.Tag.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/20/13
 * Time: 2:50 PM
 */
public class RespondingThread implements Runnable {
    private Socket socket;
    private DatabaseSystem databaseSystem;
    private LinkedList<NetworkCommand> allKnownCommands;

    public RespondingThread(Socket socket, DatabaseSystem databaseSystem) {
        this.socket = socket;
        this.databaseSystem = databaseSystem;

        // here we add all the commands that we want for the server to recognise
        allKnownCommands=new LinkedList<NetworkCommand>();
        allKnownCommands.add(new CRequestRawData(null,null,null));
        allKnownCommands.add(new CRequestRawTags(null,null,null));
        allKnownCommands.add(new CInsertImage(null,null));
        allKnownCommands.add(new CGetImage(null,0));
        allKnownCommands.add(new CDeleteImage(null,0));
        allKnownCommands.add(new CInsertTag(null,null,0));
        allKnownCommands.add(new CDeleteTag(null,0,0));
        allKnownCommands.add(new CGetImageHashCollectionByTag(null,null));
        allKnownCommands.add(new CGetServersResponsibleForImageHash(null,0));
        allKnownCommands.add(new CGetServersResponsibleForTagHash(null,0));
        allKnownCommands.add(new CGetServersAddresses(null));
        allKnownCommands.add(new CCloseThread(null));
        allKnownCommands.add(new CUpdateServersRanges(null,null));
        allKnownCommands.add(new CGetExistingServersSegments(null));
        allKnownCommands.add(new CMoveNode(null,null,false));
        allKnownCommands.add(new CPing(null));
    }

    @Override
    public void run() {
        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream (socket.getOutputStream());

            String commandCode=in.readUTF();

            boolean commandFound=false;
            for(NetworkCommand x: allKnownCommands)
                if(x.getCode().equals(commandCode))	{

                    x.respond(out, in, this.databaseSystem);
                    commandFound=true;
                }
            if(commandFound==false)
                System.err.println("The command was not recognized");
            in.close();
            out.close();
            socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
