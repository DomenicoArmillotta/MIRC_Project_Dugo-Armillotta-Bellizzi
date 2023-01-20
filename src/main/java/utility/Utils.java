package utility;

import fileManager.ConfigurationParameters;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class Utils {
    //method used to concatenate two byte arrays
    public static byte[] addByteArray(byte[] array1, byte[] array2){
        byte[] concatenatedArray = new byte[array1.length + array2.length];
        System.arraycopy(array1, 0, concatenatedArray, 0, array1.length);
        System.arraycopy(array2, 0, concatenatedArray, array1.length, array2.length);
        return concatenatedArray;
    }

    public static int getDocLen(FileChannel channel, String key) throws IOException {
        int docLen = 0;
        int entrySize = ConfigurationParameters.DOC_INDEX_ENTRY_SIZE;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
        int lowerBound = 0;
        int upperBound = (int) channel.size()-entrySize;
        while (lowerBound <= upperBound) {
            int midpoint = (lowerBound + upperBound) / 2;
            if(midpoint%entrySize!=0){
                midpoint += midpoint%entrySize;
            }
            buffer.position(midpoint);
            ByteBuffer ba = ByteBuffer.allocate(10);
            buffer.get(ba.array(), 0, 10);
            String value = Text.decode(ba.array());
            value = value.replaceAll("\0", "");
            if (value.equals(key)) {
                ByteBuffer bf1 = ByteBuffer.allocate(4);
                buffer.get(bf1.array(), 0, 4);
                docLen = bf1.getInt();
                break;
            } else if (Integer.parseInt(key) - Integer.parseInt(value) < 0) {
                upperBound = midpoint - entrySize;
            } else {
                lowerBound = midpoint + entrySize;
            }
        }
        return docLen;
    }

}
