package NetworkInfrastructure;

import ConsistentHashing.HelpingClasses.ServerSegmentsStruct;
import Data.DatabaseSystem;
import NetworkInfrastructure.NetworkCommands.NetworkCommand;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Random;

public class IncomingConnectionsThread implements Runnable {

    private LinkedList<NetworkCommand> CommandsQueueLoad;
    private DatabaseSystem databaseSystem;
    private boolean isAlive;
    private Random rand;

    public IncomingConnectionsThread(DatabaseSystem databaseSystem) {
        this.databaseSystem=databaseSystem;
        CommandsQueueLoad=new LinkedList<NetworkCommand>();
        this.isAlive=true;
        this.rand=new Random();
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
                // we respond in a new Thread
                new Thread(new RespondingThread(socket,
                        this.databaseSystem)).start();

                this.checkForNonResponsiveServers();
            }
            serverSocket.close();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void checkForNonResponsiveServers() {
        LinkedList<ServerSegmentsStruct> existingServers =
                this.databaseSystem.getDistributionManager().getExistingServers();
        if( existingServers.getFirst().serverNetworkInfo.equals(
                this.databaseSystem.getServerNetworkInfo()  ) &&
                    this.rand.nextInt( existingServers.size() *1) == 0)
            new Thread(new PingThread(existingServers,this.databaseSystem)).start();
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
