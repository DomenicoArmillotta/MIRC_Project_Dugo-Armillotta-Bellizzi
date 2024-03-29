package utility;

import fileManager.ConfigurationParameters;
import invertedIndex.LexiconEntry;
import invertedIndex.LexiconStats;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

/**
 * Class that put together all those auxiliary methods needed
 */
public class Utils {
    /**
     * Method to concatanate two byte arrays
     * @param array1 first array
     * @param array2 second array
     * @return resulting array
     */
    public static byte[] addByteArray(byte[] array1, byte[] array2){
        byte[] concatenatedArray = new byte[array1.length + array2.length];
        System.arraycopy(array1, 0, concatenatedArray, 0, array1.length);
        System.arraycopy(array2, 0, concatenatedArray, array1.length, array2.length);
        return concatenatedArray;
    }

    /**
     * Method to take out bytes from string
     * @param term term from which byte are taken
     * @return lexicon Byte
     */
    public static byte[] getBytesFromString(String term){
        Text key = new Text(term);
        byte[] lexiconBytes;
        //we check if the string is greater than 20 chars, in that case we truncate it
        if(key.getLength()>=21){
            Text truncKey = new Text(term.substring(0,20));
            lexiconBytes = truncKey.getBytes();
        }
        else{
            //bytes for the Text object are allocated, which is a string of 20 chars
            lexiconBytes = ByteBuffer.allocate(22).put(key.getBytes()).array();
        }
        return lexiconBytes;
    }

    /**
     * Method to create an entry in Lexicon in byte format
     * @param dF document frequency
     * @param cF collection frequency
     * @param docLen document lenght
     * @param tfLen tf lenght
     * @param offsetDocs  offset of docids
     * @param offsetTfs offset of tfs
     * @param idf
     * @param tup term upper bound for bm25
     * @param tuptfidf term upper bound tfidf
     * @param offsetSkip
     * @param skipLen
     * @return lexicon entry in byte[]
     */
    public static byte[] createLexiconEntry(int dF, long cF, int docLen, int tfLen, long offsetDocs, long offsetTfs, double idf, double tup, double tuptfidf, long offsetSkip, int skipLen){
        //take the document frequency
        byte[] dfBytes = ByteBuffer.allocate(4).putInt(dF).array();
        //take the collection frequency
        byte[] cfBytes = ByteBuffer.allocate(8).putLong(cF).array();
        //take list dim for both docids and tfs
        byte[] docBytes = ByteBuffer.allocate(4).putInt(docLen).array();
        byte[] tfBytes = ByteBuffer.allocate(4).putInt(tfLen).array();
        //take the offset of docids
        byte[] offsetDocBytes = ByteBuffer.allocate(8).putLong(offsetDocs).array();
        //take the offset of tfs
        byte[] offsetTfBytes = ByteBuffer.allocate(8).putLong(offsetTfs).array();
        //take the idf
        byte[] idfBytes = ByteBuffer.allocate(8).putDouble(idf).array();
        //initialize the bytes for other info (not available now)
        byte[] tupBytes = ByteBuffer.allocate(8).putDouble(tup).array();
        byte[] tupTfIdfBytes = ByteBuffer.allocate(8).putDouble(tuptfidf).array();
        byte[] offsetSkipBytes = ByteBuffer.allocate(8).putLong(offsetSkip).array();
        byte[] skipBytes = ByteBuffer.allocate(4).putInt(skipLen).array();
        //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
        byte[] lexiconBytes = dfBytes;
        lexiconBytes = addByteArray(lexiconBytes,cfBytes);
        lexiconBytes = addByteArray(lexiconBytes,docBytes);
        lexiconBytes = addByteArray(lexiconBytes,tfBytes);
        lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
        lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
        lexiconBytes = addByteArray(lexiconBytes,idfBytes);
        lexiconBytes = addByteArray(lexiconBytes,tupBytes);
        lexiconBytes = addByteArray(lexiconBytes,tupTfIdfBytes);
        lexiconBytes = addByteArray(lexiconBytes,offsetSkipBytes);
        lexiconBytes = addByteArray(lexiconBytes,skipBytes);
        return lexiconBytes;
    }

    /**
     * create skip info in byte[] ready to be written in the skipInfo.txt file
     * @param docId
     * @param docBytes
     * @param tfBytes
     * @return skip info in byte[]
     */
    public static byte[] createSkipInfoBlock(int docId, int docBytes, int tfBytes){
        byte[] endDocidBytes = ByteBuffer.allocate(4).putInt(docId).array();
        //System.out.println("End docid bytes: " + docid + " " + endDocidBytes.length);
        byte[] numBytes = ByteBuffer.allocate(4).putInt(docBytes).array();
        //System.out.println("Bytes docid: " + docid + " " + nBytes);
        byte[] numTfBytes = ByteBuffer.allocate(4).putInt(tfBytes).array();
        //System.out.println("Bytes tf: " + docid + " " + tfBytes);
        endDocidBytes = addByteArray(endDocidBytes,numBytes);
        endDocidBytes = addByteArray(endDocidBytes,numTfBytes);
        return endDocidBytes;
    }

    /**
     * get lexicon entry given the file channel and the offset
     * @param channel input file channel
     * @param offset of the lexicon entry
     * @return the structure lexicon entry for the selected term
     * @throws IOException
     */
    public static LexiconEntry getLexiconEntry(FileChannel channel, long offset) throws IOException {
        int entrySize = ConfigurationParameters.LEXICON_ENTRY_SIZE;
        int keySize = ConfigurationParameters.LEXICON_KEY_SIZE;
        ByteBuffer readBuffer = ByteBuffer.allocate(entrySize);
        //we set the position in the files using the offsets
        channel.position(offset);
        channel.read(readBuffer);
        readBuffer.position(0);
        //read first keySize bytes for the term
        ByteBuffer term = ByteBuffer.allocate(keySize);
        readBuffer.get(term.array(), 0, keySize);
        //read remaining bytes for the lexicon stats
        ByteBuffer val = ByteBuffer.allocate(entrySize-keySize);
        readBuffer.get(val.array(), 0, entrySize-keySize);
        //we use a method for reading the 36 bytes in a LexiconStats object
        LexiconStats lexiconStats = new LexiconStats(val);
        //convert the bytes to the String
        String word = Text.decode(term.array());
        //replace null characters
        word = word.replaceAll("\0", "");
        return new LexiconEntry(word, lexiconStats);
    }

    /**
     * creation of the parameters document containing :
     * avgLenBytes , totLenBytes , numDocsBytes
     * @param totalLength
     * @param numDocs
     * @throws IOException
     */
    public static void createDocumentStatistics(double totalLength, double numDocs) throws IOException {
        double averageDocLen = totalLength/numDocs;
        RandomAccessFile outFile = new RandomAccessFile(new File("docs/parameters.txt"), "rw");
        FileChannel outChannel = outFile.getChannel();
        //System.out.println(averageDocLen + " " + totalLength + " " + numDocs);
        byte[] avgLenBytes = ByteBuffer.allocate(8).putDouble(averageDocLen).array();
        //take the offset of docids
        byte[] totLenBytes = ByteBuffer.allocate(8).putDouble(totalLength).array();
        //take the offset of tfs
        byte[] numDocsBytes = ByteBuffer.allocate(8).putDouble(numDocs).array();
        //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
        avgLenBytes = addByteArray(avgLenBytes,totLenBytes);
        avgLenBytes = addByteArray(avgLenBytes,numDocsBytes);
        //write statistics to disk
        ByteBuffer outBuf = ByteBuffer.allocate(avgLenBytes.length);
        outBuf.put(avgLenBytes);
        outBuf.flip();
        outChannel.write(outBuf);
        outChannel.close();
    }


    /**
     * used to retrieve the lexicon-stats of the term from the lexicon
     * @param channel file channel of the lexicon
     * @param key term of interest
     * @return lexicon stats (info of the term in the lexicon)
     * @throws IOException
     */
    public static LexiconStats getPointer(FileChannel channel, String key) throws IOException {
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
                //System.out.println("Found key " + key + " at position " + midpoint);
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




}
