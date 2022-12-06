package invertedIndex;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.elsa.ElsaSerializerBase;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByKey;
import static java.util.stream.Collectors.toMap;

public class InvertedIndex {

    private DB db;
    private String outPath;
    private Map<String, Integer> lexicon;
    private List<String> sortedTerms;
    private List<List<Posting>> invIndex;
    private List<byte[]> docids;
    private List<byte[]> tfs;
    private int nList = 0; //pointer of the list for a term in the lexicon

    public InvertedIndex(int n){
        outPath = "_"+n;
        db = DBMaker.fileDB("docs/index"+n+".db").make();
        lexicon = db.hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        invIndex = (List<List<Posting>>)db.indexTreeList("invIndex", Serializer.JAVA).createOrOpen();
        docids = db.indexTreeList("invIndex", Serializer.BYTE_ARRAY).createOrOpen();
        tfs = db.indexTreeList("invIndex", Serializer.BYTE_ARRAY).createOrOpen();
        sortedTerms = db.indexTreeList("sortedTerms", Serializer.STRING).createOrOpen();

    }

    //TODO: update statistics of lexicon

    public void addPosting(String term, int docid, int freq) throws IOException {
        List<Posting> pl = new ArrayList<>();
        if(lexicon.get(term) != null){
            pl = invIndex.get(lexicon.get(term));
            for (int i = 0; i < pl.size(); i++) {
                if (pl.get(i).getDocumentId() == docid) {
                    pl.get(i).addOccurrence();
                    invIndex.set(lexicon.get(term),pl);
                    return;
                }
            }
            pl.add(new Posting(docid, freq));
            invIndex.set(lexicon.get(term),pl);
        }
        else{
            lexicon.put(term, nList);
            nList++;
            pl.add(new Posting(docid, freq));
            invIndex.add(pl);
        }
        //METODO ALTERNATIVO (potrebbe essere il definitivo)
        //TODO: ricorda per dopo, i byte sono con il segno, quando li leggerai, devono essere senza segno
        /*
        if(lexicon.get(term) != null){
            byte[] d = docids.get(lexicon.get(term));
            byte[] t = tfs.get(lexicon.get(term));
            int b = 0;
            for(int i = 0; i < d.length; i++){
            //while((b = new ByteArrayInputStream(d).read()) != -1){
                b = new ByteArrayInputStream(d).read();
                if (b == docid) {
                    int curFreq = t[i];
                    curFreq++;
                    t[i] = (byte) curFreq;
                    tfs.set(lexicon.get(term), t);
                    //update tf
                    return;
                }
            }
            byte[] doc = ByteBuffer.allocate(4).putInt(docid).array();
            byte[] out = new byte[d.length + doc.length];
            System.arraycopy(d, 0, out, 0, d.length);
            System.arraycopy(doc, 0, out, d.length, doc.length);
            docids.set(lexicon.get(term),out);
            byte[] tf = ByteBuffer.allocate(4).putInt(freq).array();
            byte[] outFreq = new byte[t.length + tf.length];
            System.arraycopy(t, 0, outFreq, 0, t.length);
            System.arraycopy(tf, 0, outFreq, t.length, tf.length);
            tfs.set(lexicon.get(term),outFreq);
        }
        else{
            lexicon.put(term, nList);
            byte[] doc = ByteBuffer.allocate(4).putInt(docid).array();
            docids.add(doc);
            byte[] tf = ByteBuffer.allocate(4).putInt(freq).array();
            docids.add(tf);
        }
        */
    }

    //public void addToLexicon(String term){lexicon.put(term, 0);}

    public void sortTerms() {
        sortedTerms = lexicon.keySet().stream().sorted().collect(Collectors.toList());
        System.out.println(sortedTerms);
    }

    public void writePostings() throws IOException {
        //db.commit();
        /*List<Posting> list = invIndex.get(lexicon.get("bile"));
        List<Posting> list2 = invIndex.get(lexicon.get("american"));
        System.out.println(list);
        System.out.println(list2);*/
        /*File lexFile = new File("lexicon"+outPath);
        File docFile = new File("docids"+outPath);
        File tfFile = new File("tfs"+outPath);
        RandomAccessFile streamLex = new RandomAccessFile(lexFile, "rw");
        RandomAccessFile streamDocs = new RandomAccessFile(docFile, "rw");
        RandomAccessFile streamTf = new RandomAccessFile(tfFile, "rw");;
        FileChannel lexChannel = streamLex.getChannel();
        FileChannel docChannel = streamDocs.getChannel();
        FileChannel tfChannel = streamTf.getChannel();

        int offsetDocs = 0;
        int offsetTfs = 0;
        for(String term : sortedTerms){
            int index = lexicon.get(term);
            LexiconStats stats = new LexiconStats();
            List<Posting> pl = invIndex.get(index);
            int docLen = 0;
            int posLen = 0;
            for(Posting p: pl){
                //df: sum of number of docids
                //cd: sum of tfs
                //take the posting list
                //write posting list
                int docid = p.getDocumentId();
                int tf = p.getTermFrequency();
                byte[] baDocs = ByteBuffer.allocate(4).putInt(docid).array();
                ByteBuffer bufferValue = ByteBuffer.allocate(baDocs.length).putInt(docid);
                bufferValue.put(baDocs);
                bufferValue.flip();
                docChannel.write(bufferValue);
                byte[] baFreqs = ByteBuffer.allocate(4).putInt(tf).array();
                ByteBuffer bufferFreq = ByteBuffer.allocate(baFreqs.length);
                bufferFreq.put(baFreqs);
                bufferFreq.flip();
                tfChannel.write(bufferFreq);
                stats.setCf(stats.getCf()+tf);
                stats.setdF(stats.getdF()+1);
            }
            //take the offset of docids
            //take the offset of tfs
            //take list dim for both docids and tfs
            //write lexicon to disk
        }*/
        /*File file = new File(outPath);
        try (RandomAccessFile stream = new RandomAccessFile(file, "rw");
             FileChannel channel = stream.getChannel()) {
            byte[] ba;
            System.out.println(ba.length);
            ByteBuffer bufferValue0 = ByteBuffer.allocate(ba.length);
            bufferValue0.put(ba);
            bufferValue0.flip();
            channel.write(bufferValue0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/
        db.close();
    }
}

