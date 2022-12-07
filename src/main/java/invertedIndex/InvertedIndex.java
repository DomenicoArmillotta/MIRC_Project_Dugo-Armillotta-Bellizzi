package invertedIndex;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.*;
import java.nio.ByteBuffer;
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
    private int nList = 0; //pointer of the list for a term in the lexicon

    public InvertedIndex(int n){
        outPath = "_"+n;
        db = DBMaker.fileDB("docs/index"+n+".db").make();
        lexicon = db.hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        invIndex = (List<List<Posting>>)db.indexTreeList("invIndex", Serializer.JAVA).createOrOpen();
        sortedTerms = db.indexTreeList("sortedTerms", Serializer.STRING).createOrOpen();

    }

    //TODO: update statistics of lexicon
    //TODO: add compression

    public void addPosting(String term, int docid, int freq) throws IOException {
        List<Posting> pl = new ArrayList<>();
        byte[] doc = ByteBuffer.allocate(4).putInt(docid).array();
        byte[] tf = ByteBuffer.allocate(4).putInt(freq).array();
        if(lexicon.get(term) != null){
            pl = invIndex.get(lexicon.get(term));
            for (int i = 0; i < pl.size(); i++) {
                if (ByteBuffer.wrap(pl.get(i).getDocid()).getInt() == docid) {
                    byte[] curTf = pl.get(i).getTf();
                    int oldTf = ByteBuffer.wrap(curTf).getInt();
                    oldTf++;
                    pl.get(i).setTf(ByteBuffer.allocate(4).putInt(oldTf).array());
                    invIndex.set(lexicon.get(term),pl);
                    return;
                }
            }
            pl.add(new Posting(doc, tf));
            invIndex.set(lexicon.get(term),pl);
        }
        else{
            lexicon.put(term, nList);
            nList++;
            pl.add(new Posting(doc, tf));
            invIndex.add(pl);
        }

    }

    //public void addToLexicon(String term){lexicon.put(term, 0);}

    public void sortTerms() {
        sortedTerms = lexicon.keySet().stream().sorted().collect(Collectors.toList());
        System.out.println(sortedTerms);
    }

    public void writePostings() throws IOException {
        db.commit();
        List<Posting> list = invIndex.get(lexicon.get("bile"));
        List<Posting> list2 = invIndex.get(lexicon.get("american"));
        System.out.println(list);
        System.out.println(list2);
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

