package fileManager;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Class to takes some length information on documents
 */
public class ConfigurationParameters {

    public static final int LEXICON_ENTRY_SIZE = 94; //size of the entry of the lexicon
    public static final int DOC_INDEX_ENTRY_SIZE = 18; //size of the entry of the document index

    /**
     * Calculates average length of a document
     * @return
     * @throws IOException
     */
    public static double getAverageDocumentLength() throws IOException {
        RandomAccessFile outFile = new RandomAccessFile(new File("docs/parameters.txt"), "rw");
        FileChannel outChannel = outFile.getChannel();
        int pos = 0;
        outChannel.position(pos);
        ByteBuffer avgLen = ByteBuffer.allocate(8);
        outChannel.read(avgLen);
        outChannel.position(pos);
        avgLen.flip();
        return avgLen.getDouble();
    }

    /**
     * Get total document lenght
     * @return
     * @throws IOException
     */
    public double getTotalDocumentLength() throws IOException {
        RandomAccessFile outFile = new RandomAccessFile(new File("docs/parameters.txt"), "rw");
        FileChannel outChannel = outFile.getChannel();
        int pos = 8;
        outChannel.position(pos);
        ByteBuffer totLen = ByteBuffer.allocate(8);
        outChannel.read(totLen);
        outChannel.read(totLen);
        outChannel.position(pos);
        totLen.flip();
        return totLen.getDouble();
    }

    /**
     * Get number of documents
     * @return
     * @throws IOException
     */
    public static double getNumberOfDocuments() throws IOException {
        RandomAccessFile outFile = new RandomAccessFile(new File("docs/parameters.txt"), "rw");
        FileChannel outChannel = outFile.getChannel();
        int pos = 16;
        outChannel.position(pos);
        ByteBuffer numDocs = ByteBuffer.allocate(8);
        outChannel.read(numDocs);
        outChannel.read(numDocs);
        outChannel.position(pos);
        numDocs.flip();
        return numDocs.getDouble();
    }
}
