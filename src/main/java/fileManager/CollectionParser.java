package fileManager;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import preprocessing.PreprocessDoc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

import static utility.Utils.addByteArray;


public class CollectionParser {

    private static double totalLength = 0;
    private static double numDocs = 0;
    private DB db;
    private HTreeMap<String, Integer> documentIndex;

    //TODO: fare document index su file e non con map db!
    public void parseFile(String readPath) throws IOException {
        db = DBMaker.fileDB("docs/docIndex.db").make();
        documentIndex = db
                .hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
        File inputFile = new File(readPath);
        LineIterator it = FileUtils.lineIterator(inputFile, "UTF-8");
        int indexBlock = 0;
            try {
            while (it.hasNext()) {
                PreprocessDoc preprocessDoc = new PreprocessDoc();
                String doc = it.nextLine();
                int cont = 0;
                String[] parts = doc.split("\t");
                String docno = parts[0];
                String doc_corpus = parts[1];
                List<String> pro_doc = preprocessDoc.preprocess_doc_optimized(doc_corpus);
                //read the terms and count the length of the document
                for (String term : pro_doc) {
                    cont++;
                }
                totalLength+=cont;
                numDocs++;
                documentIndex.put(docno, cont);
            }
        }finally {
            LineIterator.closeQuietly(it);
        }
        db.commit();
        db.close();
        //compute the average document length
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
        //write lexicon entry to disk
        ByteBuffer outBuf = ByteBuffer.allocate(avgLenBytes.length);
        outBuf.put(avgLenBytes);
        outBuf.flip();
        outChannel.write(outBuf);
        outChannel.close();
    }

}
