package networkInfrastructure;

import Data.DatabaseSystem;
import networkInfrastructure.NetworkCommands.*;
import networkInfrastructure.NetworkCommands.Image.CDeleteImage;
import networkInfrastructure.NetworkCommands.Image.CGetImage;
import networkInfrastructure.NetworkCommands.Image.CGetServersResponsibleForImageHash;
import networkInfrastructure.NetworkCommands.Image.CInsertImage;
import networkInfrastructure.NetworkCommands.Tag.CDeleteTag;
import networkInfrastructure.NetworkCommands.Tag.CGetImageHashCollectionByTag;
import networkInfrastructure.NetworkCommands.Tag.CGetServersResponsibleForTagHash;
import networkInfrastructure.NetworkCommands.Tag.CInsertTag;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

public class IncomingConnectionsThread implements Runnable {

    private LinkedList<NetworkCommand> CommandsQueueLoad;
    private DatabaseSystem databaseSystem;
    private LinkedList<NetworkCommand> allKnownCommands;
    private boolean isAlive;

    public IncomingConnectionsThread(DatabaseSystem databaseSystem) {
        this.databaseSystem=databaseSystem;
        allKnownCommands=new LinkedList<NetworkCommand>();
        CommandsQueueLoad=new LinkedList<NetworkCommand>();
        this.isAlive=true;

        // here we add all the commands that we want for the server to recognise
        allKnownCommands.add(new CRequestRawData(null,null,null));
        allKnownCommands.add(new CInsertImage(null,null));
        allKnownCommands.add(new CGetServerOperationsLoadNumber(null));
        allKnownCommands.add(new CGetImage(null,0));
        allKnownCommands.add(new CDeleteImage(null,0));
        allKnownCommands.add(new CInsertTag(null,null,0));
        allKnownCommands.add(new CDeleteTag(null,null,0));
        allKnownCommands.add(new CGetImageHashCollectionByTag(null,null));
        allKnownCommands.add(new CGetServersResponsibleForImageHash(null,0));
        allKnownCommands.add(new CGetServersResponsibleForTagHash(null,0));
        allKnownCommands.add(new CGetServersAddresses(null));
        allKnownCommands.add(new CCloseThread(null));
    }

    @Override
    public void run() {
        ServerSocket serverSocket=null;
        try {
            serverSocket= new ServerSocket(
                    this.databaseSystem.getServerNetworkInfo().getPort()
                            );

            Socket socket=null;
            while ( this.isThreadAlive() ) {
                socket= serverSocket.accept();
                DataInputStream in = new DataInputStream (socket.getInputStream());
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
            }
            serverSocket.close();
        }catch (IOException e) {

            e.printStackTrace();
        }
    }

    public void setAlive(boolean alive) {
        isAlive = alive;
    }

    public boolean isThreadAlive() {
        return this.isAlive;
    }

    public int getCommandsQueueLoadNumber() {
        return CommandsQueueLoad.size();
    }
}
