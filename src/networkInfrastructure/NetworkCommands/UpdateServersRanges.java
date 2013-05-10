/*

package networkInfrastructure.NetworkCommands;

import ConsistentHashing.DistributionManager;
import Data.DatabaseSystem;
import networkInfrastructure.ServerNetworkInfo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.LinkedList;

public class UpdateServersRanges implements NetworkCommand {

	@Override
	public String getCode() {		
		return "UpdateServersRanges";
	}

    @Override
    public void request(ServerNetworkInfo serverNetworkInfo) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void respond(DatabaseSystem databaseSystem) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
*//*


    @Override
	public void request(Socket socket, DistributionManager current) {
		PrintWriter out;
		try {
			out = new PrintWriter ( socket . getOutputStream (), true );
			BufferedReader in = new BufferedReader ( new InputStreamReader (socket . getInputStream ()));

			String data;
			// We add the command code
			data = this.getCode()+"\n";
			
			// and we add the ranges
			String rangesToString=new String();
			LinkedList<ServerNetworkInfo> servers=current.getServers();
			for(ServerNetworkInfo x : servers) {
				rangesToString+=x.toString()+"/";
			}
			data+=rangesToString;
			
			// We send the data
			out . println ( data );

			// We wait for the response to know it was successfull
			String answer =in. readLine ();
			if( ! answer.equals("1"))
				System.out.println("The UpdateServersRanges command failed ");				
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Override
	public void respond(Socket socket,
                        BufferedReader in, DistributionManager current) {
		try {
			PrintWriter out = new PrintWriter ( socket . getOutputStream () , true );
			
			LinkedList<ServerNetworkInfo> ranges=new LinkedList<ServerNetworkInfo>();
			String dataReceived =in. readLine () ;
			String[] rangesAsStrings=dataReceived.split("/");
			
			for(String x : rangesAsStrings)
				ranges.add(new ServerNetworkInfo(x));
			
			//we update the current ranges in this server
		//	current.setServers(ranges);
			
			// We return that the update was succesfull;
			out . println ( "1" );
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}

*/
