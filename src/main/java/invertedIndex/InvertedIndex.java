package invertedIndex;

import compression.Compressor;
import fileManager.ConfigurationParameters;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.C;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import queryProcessing.Scorer;
import utility.Utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.Map.Entry.comparingByKey;
import static java.util.stream.Collectors.toMap;
import static utility.Utils.addByteArray;

public class InvertedIndex {
    private String outPath;
    private Map<String, LexiconStats> lexicon; //map for the lexicon: the entry are the term + the statistics for each term
    private List<String> sortedTerms; //map for the lexicon: the entry are the term + the statistics for each term
    private List<List<Posting>> invIndex; //pointers of the inverted list, one for each term
    private int nList = 0; //pointer of the list for a term in the lexicon
    public InvertedIndex(int n){
        outPath = "_"+n;
        lexicon = new HashMap<>();
        invIndex = new ArrayList<>();
    }

    public void addPosting(String term, int docid, int freq) throws IOException {
        List<Posting> pl = new ArrayList<>();
        //check if the posting list for this term has already been created
        if(lexicon.get(term) != null){
            LexiconStats l = lexicon.get(term); //get the pointer of the list
            l.setCf(l.getCf()+1); //update collection frequency
            pl = invIndex.get(lexicon.get(term).getIndex()); //get the list
            if(l.getCurdoc() == docid){
                int oldTf = l.getCurTf(); //get the current term frequency value of the term
                oldTf++; //increase term frequency by one
                //update data structures
                l.setCurTf(oldTf);
                invIndex.get(lexicon.get(term).getIndex()).get(pl.size()-1).setTf(oldTf);
                lexicon.put(term, l);
                return; //we already had the given docID so we exit after updating the term and collection frequency
            }
            l.setdF(l.getdF()+1); //update document frequency, since this docID was not present before in the list
            l.setCurdoc(docid);
            l.setCurTf(freq);
            //update data structures
            invIndex.get(lexicon.get(term).getIndex()).add(new Posting(docid, 1));
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
            pl.add(new Posting(docid, 1)); //add posting to the new list
            invIndex.add(pl); //insert the new list in the inverted index
        }
    }


    public void sortTerms() {
        //sort the lexicon by key putting it in the sortedTerms list
        sortedTerms = lexicon.keySet().stream().sorted().collect(Collectors.toList());
    }

    public void writePostings() throws IOException {
        /*List<Posting> list = invIndex.get(lexicon.get("bile").getIndex());
        List<Posting> list2 = invIndex.get(lexicon.get("american").getIndex());
        System.out.println(lexicon.get("bile").getCf() + " " + lexicon.get("bile").getdF());
        System.out.println(list);
        System.out.println(lexicon.get("american").getCf() + " " + lexicon.get("american").getdF());
        System.out.println(list2);
        List<Posting> list3 = invIndex.get(lexicon.get("lime").getIndex());
        System.out.println(lexicon.get("lime").getCf() + " " + lexicon.get("lime").getdF());
        System.out.println(list3);*/
        File lexFile = new File("docs/lexicon"+outPath+".txt");
        File docFile = new File("docs/docids"+outPath+".txt");
        File tfFile = new File("docs/tfs"+outPath+".txt");
        RandomAccessFile streamLex = new RandomAccessFile(lexFile, "rw");
        RandomAccessFile streamDocs = new RandomAccessFile(docFile, "rw");
        RandomAccessFile streamTf = new RandomAccessFile(tfFile, "rw");;
        FileChannel lexChannel = streamLex.getChannel();
        FileChannel docChannel = streamDocs.getChannel();
        FileChannel tfChannel = streamTf.getChannel();
        ConfigurationParameters cp = new ConfigurationParameters();
        Compressor compressor = new Compressor();
        double N = cp.getNumberOfDocuments();
        int offsetDocs = 0;
        int offsetTfs = 0;
        for(String term : sortedTerms){
            LexiconStats l = lexicon.get(term);
            int index = l.getIndex();
            List<Posting> pl = invIndex.get(index);
            int docLen = 0;
            int tfLen = 0;
            //idf value
            long nn = l.getdF(); // number of documents that contain the term t
            double idf = Math.log((N/nn));
            for(Posting p: pl){ //take the posting list
                //write posting list to file
                byte[] baDocs = compressor.variableByteEncodeNumber(p.getDocid()); //compress the docid with variable byte
                ByteBuffer bufferValue = ByteBuffer.allocate(baDocs.length);
                bufferValue.put(baDocs);
                bufferValue.flip();
                docChannel.write(bufferValue);
                byte[] baFreqs = compressor.unaryEncode(p.getTf()); //compress the term frequency with unary
                ByteBuffer bufferFreq = ByteBuffer.allocate(baFreqs.length);
                bufferFreq.put(baFreqs);
                bufferFreq.flip();
                tfChannel.write(bufferFreq);
                docLen+= baDocs.length;
                tfLen+= baFreqs.length;
            }
            //we check if the string is greater than 20 chars, in that case we truncate it
            Text key = new Text(term);
            byte[] lexiconBytes;
            if(key.getLength()>=21){
                Text truncKey = new Text(term.substring(0,20));
                lexiconBytes = truncKey.getBytes();
            }
            else{ //we allocate 22 bytes for the Text object, which is a string of 20 chars
                lexiconBytes = ByteBuffer.allocate(22).put(key.getBytes()).array();
            }
            lexiconBytes = addByteArray(lexiconBytes, Utils.createLexiconEntry(l.getdF(), l.getCf(), docLen, tfLen, offsetDocs, offsetTfs, idf, 0.0, 0.0, 0, 0));
            //take the document frequency
            /*byte[] dfBytes = ByteBuffer.allocate(4).putInt(l.getdF()).array();
            //take the collection frequency
            byte[] cfBytes = ByteBuffer.allocate(8).putLong(l.getCf()).array();
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
            byte[] tupBytes = ByteBuffer.allocate(8).putDouble(0.0).array();
            byte[] offsetSkipBytes = ByteBuffer.allocate(8).putLong(0).array();
            byte[] skipBytes = ByteBuffer.allocate(4).putInt(0).array();
            //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
            lexiconBytes = addByteArray(lexiconBytes,dfBytes);
            lexiconBytes = addByteArray(lexiconBytes,cfBytes);
            lexiconBytes = addByteArray(lexiconBytes,docBytes);
            lexiconBytes = addByteArray(lexiconBytes,tfBytes);
            lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
            lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
            lexiconBytes = addByteArray(lexiconBytes,idfBytes);
            lexiconBytes = addByteArray(lexiconBytes,tupBytes);
            lexiconBytes = addByteArray(lexiconBytes,offsetSkipBytes);
            lexiconBytes = addByteArray(lexiconBytes,skipBytes);*/
            //write lexicon entry to disk
            ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
            bufferLex.put(lexiconBytes);
            bufferLex.flip();
            lexChannel.write(bufferLex);
            bufferLex.clear();
            //update offsets
            offsetDocs+=docLen;
            offsetTfs+=tfLen;
        }
        lexChannel.close();
        docChannel.close();
        tfChannel.close();
    }
}

