package indexing;

import fileManager.ConfigurationParameters;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;

public class DocumentIndex {
    private HashMap<Integer, Integer> docIndex;

    public DocumentIndex() throws IOException {
        RandomAccessFile docIndexFile = new RandomAccessFile(new File("docs/docIndex.txt"), "rw");
        FileChannel docIndexChannel = docIndexFile.getChannel();
        docIndex = getDocumentIndex(docIndexChannel);
    }

    public HashMap<Integer, Integer> getDocIndex() {
        return docIndex;
    }

    public void setDocIndex(HashMap<Integer, Integer> docIndex) {
        this.docIndex = docIndex;
    }

    public static HashMap<Integer,Integer> getDocumentIndex(FileChannel channel) throws IOException {
        HashMap<Integer,Integer> docIndex = new HashMap<>();
        int entrySize = ConfigurationParameters.DOC_INDEX_ENTRY_SIZE;
        int keySize = ConfigurationParameters.DOC_INDEX_KEY_SIZE;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
        int lowerBound = 0;
        while(lowerBound!=channel.size()) {
            buffer.position(lowerBound);
            ByteBuffer ba = ByteBuffer.allocate(keySize);
            buffer.get(ba.array(), 0, keySize);
            if (ba.hasArray()) {
                ByteBuffer bf1 = ByteBuffer.allocate(entrySize - keySize);
                buffer.get(bf1.array(), 0, entrySize - keySize);
                int docid = bf1.getInt();
                int doclen = bf1.getInt();
                docIndex.put(docid,doclen);
            }
            lowerBound += entrySize;
        }
        return docIndex;
    }
}
