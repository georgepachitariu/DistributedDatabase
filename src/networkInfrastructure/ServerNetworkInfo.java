package NetworkInfrastructure;

public class ServerNetworkInfo {

	private String IP;
	private int port;

	public ServerNetworkInfo(String input) {
		String[] parts=input.split(":");
		this.IP=parts[0];
		this.port=Integer.parseInt(parts[1]);		
	}
	
	public ServerNetworkInfo (String ip, int port) {
			this.IP=ip;
			this.port=port;
	}

	public String getIP() {
		return IP;
	}

	public void setIP(String iP) {
		IP = iP;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String toString() {
		return this.IP+":"+this.port;
	}

    @Override
    public boolean equals(Object o) {
        if(this.toString().equals(((ServerNetworkInfo)o).toString()))
            return true;
        return false;
    }
}
