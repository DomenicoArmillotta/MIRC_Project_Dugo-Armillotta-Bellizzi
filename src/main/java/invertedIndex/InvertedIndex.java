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
    private Map<String, LexiconStats> lexicon; //map for the lexicon: the entry are the term + the statistics for each term
    private List<String> sortedTerms; //map for the lexicon: the entry are the term + the statistics for each term
    private List<List<Posting>> invIndex; //pointers of the inverted list, one for each term
    private int nList = 0; //pointer of the list for a term in the lexicon

    public InvertedIndex(int n){
        outPath = "_"+n;
        lexicon = new HashMap<>();
        invIndex = new ArrayList<>();
        /*db = DBMaker.fileDB("docs/index"+n+".db").make();
        lexicon = db.hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        invIndex = (List<List<Posting>>)db.indexTreeList("invIndex", Serializer.JAVA).createOrOpen();
        sortedTerms = db.indexTreeList("sortedTerms", Serializer.STRING).createOrOpen();*/
    }

    //TODO: add compression

    public void addPosting(String term, int docid, int freq) throws IOException {
        List<Posting> pl = new ArrayList<>();
        byte[] doc = ByteBuffer.allocate(4).putInt(docid).array(); //convert the docID to bytes
        byte[] tf = ByteBuffer.allocate(4).putInt(freq).array(); //convert the term frequency to bytes
        //check if the posting list for this term has already been created
        if(lexicon.get(term) != null){
            LexiconStats l = lexicon.get(term); //get the pointer of the list
            l.setCf(l.getCf()+1); //update collection frequency
            pl = invIndex.get(lexicon.get(term).getIndex()); //get the list
            if(l.getCurdoc() == docid){
                int oldTf = l.getCurTf(); //get the current term frequency value of the term
                oldTf++; //increase term frequency by one
                //pl.get(pl.size()-1).setTf(ByteBuffer.allocate(4).putInt(oldTf).array());
                byte[] newTf = ByteBuffer.allocate(4).putInt(oldTf).array(); //update term frequency
                //update data structures
                l.setCurTf(oldTf);
                invIndex.get(lexicon.get(term).getIndex()).get(pl.size()-1).setTf(newTf);
                lexicon.put(term, l);
                return; //we already had the given docID so we exit after updating the term and collection frequency
            }
            l.setdF(l.getdF()+1); //update document frequency, since this docID was not present before in the list
            l.setCurdoc(docid);
            l.setCurTf(freq);
            //pl.add(new Posting(doc, tf)); //add the posting to the list
            //update data structures
            invIndex.get(lexicon.get(term).getIndex()).add(new Posting(doc, tf));
            lexicon.put(term, l);
        }
        else{ //create new posting list
            LexiconStats l = new LexiconStats();
            //initialize the lexicon statistics for the term and add it to the lexicon
            l.setIndex(nList);
            l.setCf(1); //initialize collection frequency to 1
            l.setdF(1); //initialize document frequency by 1
            l.setCurdoc(docid); //set the current document id
            l.setCurTf(freq); //set the current term frequency
            lexicon.put(term, l);
            nList++; //increase the pointer for the next list
            pl.add(new Posting(doc, tf)); //add posting to the new list
            invIndex.add(pl); //insert the new list in the inverted index
        }
        /*List<Posting> pl = new ArrayList<>();
        byte[] doc = ByteBuffer.allocate(4).putInt(docid).array(); //convert the docID to bytes
        byte[] tf = ByteBuffer.allocate(4).putInt(freq).array(); //convert the term frequency to bytes
        //check if the posting list for this term has already been created
        if(lexicon.get(term) != null){
            LexiconStats l = lexicon.get(term); //get the pointer of the list
            l.setCf(l.getCf()+1); //update collection frequency
            pl = invIndex.get(lexicon.get(term).getIndex()); //get the list
            if(l.getCurdoc() == docid){
                int oldTf = l.getCurTf(); //get the current term frequency value of the term
                oldTf++; //increase term frequency by one
                pl.get(pl.size()-1).setTf(ByteBuffer.allocate(4).putInt(oldTf).array()); //update term frequency
                //update data structures
                l.setCurTf(oldTf);
                invIndex.set(lexicon.get(term).getIndex(),pl);
                lexicon.put(term, l);
                return; //we already had the given docID so we exit after updating the term and collection frequency
            }
            l.setdF(l.getdF()+1); //update document frequency, since this docID was not present before in the list
            l.setCurdoc(docid);
            l.setCurTf(freq);
            pl.add(new Posting(doc, tf)); //add the posting to the list
            //update data structures
            invIndex.set(lexicon.get(term).getIndex(),pl);
            lexicon.put(term, l);
        }
        else{ //create new posting list
            LexiconStats l = new LexiconStats();
            //initialize the lexicon statistics for the term and add it to the lexicon
            l.setIndex(nList);
            l.setCf(1); //initialize collection frequency to 1
            l.setdF(1); //initialize document frequency by 1
            l.setCurdoc(docid); //set the current document id
            l.setCurTf(freq); //set the current term frequency
            lexicon.put(term, l);
            nList++; //increase the pointer for the next list
            pl.add(new Posting(doc, tf)); //add posting to the new list
            invIndex.add(pl); //insert the new list in the inverted index
        }*/
    }

    public void sortTerms() {
        //sort the lexicon by key putting it in the sortedTerms list
        sortedTerms = lexicon.keySet().stream().sorted().collect(Collectors.toList());
    }

    public void writePostings() throws IOException {
        List<Posting> list = invIndex.get(lexicon.get("bile").getIndex());
        List<Posting> list2 = invIndex.get(lexicon.get("american").getIndex());
        System.out.println(lexicon.get("bile").getCf() + " " + lexicon.get("bile").getdF());
        System.out.println(list);
        System.out.println(lexicon.get("american").getCf() + " " + lexicon.get("american").getdF());
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
        //db.commit();
        //db.close();
    }
}

