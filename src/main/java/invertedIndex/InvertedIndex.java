package invertedIndex;

import compression.Compressor;
import fileManager.ConfigurationParameters;
import org.apache.hadoop.io.Text;
import utility.Utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

import static utility.Utils.addByteArray;

public class InvertedIndex {
    private String outPath;
    //map for the lexicon: the entry are the term + the statistics for each term
    private Map<String, LexiconStats> lexicon;
    private List<String> sortedTerms;
    private List<List<Posting>> invIndex; //pointers of the inverted list, one for each term
    private int nList = 0; //pointer of the list for a term in the lexicon
    public InvertedIndex(int n){
        outPath = "_"+n;
        lexicon = new HashMap<>();
        invIndex = new ArrayList<>();
    }

    /**
     * case 1 : term does not exist, adds the posting to the inverted index and in the lexicon
     * case 2 : term exists, update the posting in the inverted index (tf) and lexicon (tf)
     * @param term of the corresponding posting list
     * @param docid
     * @param freq to assign to posting
     * @throws IOException
     */
    public void addPosting(String term, int docid, int freq) throws IOException {
        List<Posting> postingList = new ArrayList<>();
        //check if the posting list for this term has already been created
        if(lexicon.get(term) != null){
            //get the stats of te term
            LexiconStats lexiconStats = lexicon.get(term);
            //update collection frequency
            lexiconStats.setCf(lexiconStats.getCf()+1);
            //get the posting list of the term
            postingList = invIndex.get(lexicon.get(term).getIndex());
            if(lexiconStats.getCurdoc() == docid){
                int oldTf = lexiconStats.getCurTf(); //get the current term frequency value of the term
                oldTf++; //update term frequency
                //update data structures
                lexiconStats.setCurTf(oldTf);
                invIndex.get(lexicon.get(term).getIndex()).get(postingList.size()-1).setTf(oldTf);
                lexicon.put(term, lexiconStats);
                //in this case already had the given docID so we exit after updating the term and collection frequency
                return;
            }
            //update document frequency, since this docID was not present before in the list
            lexiconStats.setdF(lexiconStats.getdF()+1);
            lexiconStats.setCurdoc(docid);
            lexiconStats.setCurTf(freq);
            //update data structures
            invIndex.get(lexicon.get(term).getIndex()).add(new Posting(docid, 1));
            lexicon.put(term, lexiconStats);
        }
        //create new posting list if the posting don't exist
        else{
            LexiconStats lexiconStats = new LexiconStats();
            //initialize the lexicon statistics for the term and add it to the lexicon
            lexiconStats.setIndex(nList);
            lexiconStats.setCf(1); //initialize collection frequency to 1
            lexiconStats.setdF(1); //initialize document frequency by 1
            lexiconStats.setCurdoc(docid); //set the current document id
            lexiconStats.setCurTf(freq); //set the current term frequency
            lexicon.put(term, lexiconStats);
            nList++; //increase the pointer for the next list
            postingList.add(new Posting(docid, 1)); //add posting to the new list
            invIndex.add(postingList); //insert the new list in the inverted index
        }
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
        List<Posting> list3 = invIndex.get(lexicon.get("lime").getIndex());
        System.out.println(lexicon.get("lime").getCf() + " " + lexicon.get("lime").getdF());
        System.out.println(list3);
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

