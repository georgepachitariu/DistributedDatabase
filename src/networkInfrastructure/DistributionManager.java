/*
package networkInfrastructure;

public class DistributionManager {

	private ServerNetworkInfo currentServer;
	private IncomingConnectionsThread incomingConnectionsThread;
	private LinkedList<ServerNetworkInfo> allServers;

	public DistributionManager() {
		this.currentServer=new ServerNetworkInfo("127.0.0.1", 8000);
		
		this.incomingConnectionsThread=new IncomingConnectionsThread(this);
		new Thread(incomingConnectionsThread).start();
	}
	
	public void sendServerUpdatesToAll() throws IOException {
			Socket socket= new Socket ( this.allServers.get(0).getIP() , this.allServers.get(0).getPort() );
			new UpdateServersRanges().request(socket,this);
			socket.close();
	}
	
	public LinkedList<ServerNetworkInfo> getServers() {		
		return this.allServers;
	}

	public void setServers(LinkedList<ServerNetworkInfo> ranges) {
		this.allServers=ranges;
	}

	public ServerNetworkInfo getServerInfo() {
		return this.currentServer;
	}

}*/

