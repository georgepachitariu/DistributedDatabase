package networkInfrastructure.NetworkCommands;

import Data.DatabaseSystem;
import networkInfrastructure.ServerNetworkInfo;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

public abstract class NetworkCommand {

    protected ServerNetworkInfo server;
    protected DataOutputStream out;
    protected DataInputStream in;
    private Socket socket;

    protected NetworkCommand(ServerNetworkInfo server) {
        this.server=server;
    }

    public abstract String getCode();

    public boolean request()  throws IOException {
        this.socket = new Socket();
        socket.connect(new InetSocketAddress(server.getIP(), server.getPort()));

        this.out = new DataOutputStream ( socket.getOutputStream());
        this.in = new DataInputStream (socket.getInputStream());

        out.writeUTF(this.getCode()); // We add the command code
        return false;
    }

    public void requestClose() throws IOException {
        this.out.close();
        this.in.close();
        this.socket.close();
    }

    public abstract void  respond(DataOutputStream out, DataInputStream in,
                 DatabaseSystem databaseSystem) throws IOException;


    public byte[] getAllAvailableBytesFromStream(DataInputStream stream, int expectedLength) throws IOException {
        byte []b=new byte[ (int)Math.pow(2,24) ];

        int bytesRead=0 , total=0;
        while(total <expectedLength) {
            int bytesAvailable=stream.available();
             if(bytesAvailable >0) {
                bytesRead=stream.read(b, total, bytesAvailable);
                total+=bytesRead;
            }
            else try {
                Thread.sleep(100);
            } catch (InterruptedException e) { }
        }

        return Arrays.copyOfRange(b, 0, total);

    }


}