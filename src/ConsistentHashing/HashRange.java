package ConsistentHashing;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created with IntelliJ IDEA.
 * User: George
 * Date: 4/16/13
 * Time: 4:23 PM
 */
public class HashRange {
    public long startPoint;
    public long endPoint;

    public HashRange(long start, long end) {
        this.startPoint = start;
        this.endPoint = end;
    }

    public HashRange(String raw) {
        String[] parts=raw.split("_");
        this.startPoint=Long.parseLong(parts[0]);
        this.endPoint=Long.parseLong(parts[1]);
    }

    public HashRange() {
    }

    @Override
    public String toString() {
        return String.valueOf(this.startPoint) +"_" +
                String.valueOf(this.endPoint);
    }

    public byte[] toBytes() {
        return  ByteBuffer.allocate(16).putLong(this.startPoint).putLong(this.endPoint).array();
    }

    public static HashRange createFromBytes(SocketChannel socketChannel) {

        try {
            ByteBuffer buffer=ByteBuffer.allocate(16);
            socketChannel.read(buffer);
            HashRange h=new HashRange();
            h.startPoint=buffer.getLong();
            h.endPoint=buffer.getLong();
            return h;
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return null;
    }


    public boolean contains(long imageHash) {
        if(imageHash >= this.startPoint &&
                imageHash <this.endPoint)
                    return true;
        return false;
    }
}
