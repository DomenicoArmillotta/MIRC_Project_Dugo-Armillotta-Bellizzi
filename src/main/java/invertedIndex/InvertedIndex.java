package invertedIndex;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.elsa.ElsaSerializerBase;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
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

    public void addPosting(String term, int docid, int freq){
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

    }

    //public void addToLexicon(String term){lexicon.put(term, 0);}

    public void sortTerms() {
        sortedTerms = lexicon.keySet().stream().sorted().collect(Collectors.toList());
        System.out.println(sortedTerms);
    }

    public void writePostings() {
        //TODO: declare FileChannel for three different files: one for docids, one for tfs, one for lexicon
        /*for (Map.Entry<String, Integer> entry :
                lexicon.entrySet()) {

            // put key and value separated by a colon
            System.out.println(entry.getKey() + ":"
                    + entry.getValue());
        }*/
        File lexFile = new File("lexicon"+outPath);
        File docFile = new File("docids"+outPath);
        File tfFile = new File("tfs"+outPath);
        RandomAccessFile streamLex;
        RandomAccessFile streamDocs;
        RandomAccessFile streamTf;
        FileChannel lexChannel;
        FileChannel docChannel;
        FileChannel tfChannel;
        List<Posting> list = invIndex.get(lexicon.get("bile"));

        List<Posting> list2 = invIndex.get(lexicon.get("american"));
        System.out.println(list);
        System.out.println(list2);
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

