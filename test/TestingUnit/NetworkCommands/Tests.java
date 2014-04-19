package TestingUnit.NetworkCommands;

import ConsistentHashing.HelpingClasses.HashRange;
import Data.DatabaseSystem;
import junit.framework.Assert;
import NetworkInfrastructure.NetworkCommands.CCloseThread;
import NetworkInfrastructure.IncomingConnectionsThread;
import NetworkInfrastructure.NetworkCommands.CUpdateServersRanges;
import NetworkInfrastructure.ServerNetworkInfo;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 5/20/13
 * Time: 12:52 AM
 */
public class Tests{
    @Test
    public void test_CUpdateServersRangesTest_GetRangesToBeDeleted() {

        LinkedList<HashRange> newRanges=new LinkedList<HashRange>();
        LinkedList<HashRange > oldRanges=new LinkedList<HashRange>();

        oldRanges.add(new HashRange(0,80));
        oldRanges.add(new HashRange(100,200));

        newRanges.add(new HashRange(100,200));
        newRanges.add(new HashRange(30,60));

        CUpdateServersRanges test = new CUpdateServersRanges(null,null);
        LinkedList<HashRange> result =
                test.getRangesToBeDeleted(newRanges, oldRanges);

        Assert.assertEquals(result.get(0).toString(),new HashRange(0,30).toString());
        Assert.assertEquals(result.get(1).toString(),new HashRange(60,80).toString());
        Assert.assertTrue(result.size()==2);
    }

    @Test
    public void testCCloseThread_goodScenario() throws IOException {
         ServerNetworkInfo serverNetworkInfo;
         DatabaseSystem databaseSystem;
         IncomingConnectionsThread incomingConnectionsThread;

        // creating the server
        serverNetworkInfo=mock(ServerNetworkInfo.class);
        when(serverNetworkInfo.getIP()).thenReturn("127.0.0.1");
        when(serverNetworkInfo.getPort()).thenReturn(8200);
        when(serverNetworkInfo.toString()).thenReturn("127.0.0.1:8200");

        databaseSystem = mock(DatabaseSystem.class);
        when(databaseSystem.getServerNetworkInfo()).thenReturn(serverNetworkInfo);

        incomingConnectionsThread=new IncomingConnectionsThread(databaseSystem);
        incomingConnectionsThread.setAlive(true);
        Thread thread = new Thread(incomingConnectionsThread);
        thread.start();

        when(databaseSystem.getIncomingConnectionsThread()).thenReturn(incomingConnectionsThread);

        // the client
        CCloseThread command= new CCloseThread(serverNetworkInfo);

        command.request();   // we execute the command

        org.junit.Assert.assertEquals(false, incomingConnectionsThread.isThreadAlive());
    }
}
