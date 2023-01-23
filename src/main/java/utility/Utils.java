package utility;

import fileManager.ConfigurationParameters;
import invertedIndex.LexiconStats;
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

    public static byte[] getBytesFromString(String term){
        Text key = new Text(term);
        byte[] lexiconBytes;
        if(key.getLength()>=21){
            Text truncKey = new Text(term.substring(0,20));
            lexiconBytes = truncKey.getBytes();
        }
        else{ //we allocate 22 bytes for the Text object, which is a string of 20 chars
            lexiconBytes = ByteBuffer.allocate(22).put(key.getBytes()).array();
        }
        return lexiconBytes;
    }

    public LexiconStats getPointer(FileChannel channel, String key) throws IOException {
        LexiconStats l = new LexiconStats(); //initialize lexicon stats object
        int entrySize = ConfigurationParameters.LEXICON_ENTRY_SIZE; //take entry size
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size()); //map file to memory
        int lowerBound = 0; //initialize lower bound to the start of the file
        int upperBound = (int) channel.size() - entrySize; //initialize upperbound to the offset of the last entry
        while (lowerBound <= upperBound) {
            int midpoint = (lowerBound + upperBound) / 2; //start from the center
            if(midpoint%entrySize!=0){
                midpoint += midpoint%entrySize; //add the reminder if it's not null
            }
            buffer.position(midpoint);
            ByteBuffer ba = ByteBuffer.allocate(22);
            buffer.get(ba.array(), 0, 22); //take the term bytes
            String value = Text.decode(ba.array());
            value = value.replaceAll("\0", ""); //replace null characters
            if (value.equals(key)) { //if they are equal we are done
                System.out.println("Found key " + key + " at position " + midpoint);
                ByteBuffer bf1 = ByteBuffer.allocate(entrySize-22);
                buffer.get(bf1.array(), 0, entrySize-22); //take the bytes with the information we are searching
                l = new LexiconStats(bf1);
                break;
            } else if (key.compareTo(value) < 0) {
                upperBound = midpoint - entrySize; //we move up if the word comes before
            } else {
                lowerBound = midpoint + entrySize; //we move down if the word comes after
            }
        }
        return l;
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
