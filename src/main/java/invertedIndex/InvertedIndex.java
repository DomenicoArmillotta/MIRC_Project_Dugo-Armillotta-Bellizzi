package invertedIndex;

import compression.Compressor;
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
    //list of sorted term of lexicon
    private List<String> sortedTerms;
    //pointers of the inverted list, one for each term
    private List<List<Posting>> invIndex;
    //pointer of the list for a term in the lexicon
    private int nList = 0;
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


    /**
     * sort the term of the lexicon in one list
     */
    public void sortTerms() {
        //sort the lexicon by key putting it in the sortedTerms list
        sortedTerms = lexicon.keySet().stream().sorted().collect(Collectors.toList());
    }

    //TODO: pulire e modificare i commenti (ALLA FINE!!!!!!!!)
    /**
     * is used to write each block of SPIMI on disk
     * It does two things:
     * 1. write the inverted index to file, iterating the postings of each term,
     * compresses the data and writes them to the appropriate file.
     *  We use one file for each type of data:
     * - doc_id file
     * - term frequency file
     * 2. Writes the compressed lexicon to file
     * - lexicon file
     * @throws IOException
     */
    public void writePostings() throws IOException {
        //output file for different type of data
        File lexFile = new File("docs/lexicon"+outPath+".txt");
        File docFile = new File("docs/docids"+outPath+".txt");
        File tfFile = new File("docs/tfs"+outPath+".txt");
        RandomAccessFile streamLex = new RandomAccessFile(lexFile, "rw");
        RandomAccessFile streamDocs = new RandomAccessFile(docFile, "rw");
        RandomAccessFile streamTf = new RandomAccessFile(tfFile, "rw");;
        FileChannel lexChannel = streamLex.getChannel();
        FileChannel docChannel = streamDocs.getChannel();
        FileChannel tfChannel = streamTf.getChannel();
        Compressor compressor = new Compressor();
        int offsetDocs = 0;
        int offsetTfs = 0;
        //iterate over term in the collection
        for(String term : sortedTerms){
            LexiconStats lexiconStats = lexicon.get(term);
            int index = lexiconStats.getIndex();
            List<Posting> postingList = invIndex.get(index);
            int docLen = 0;
            int tfLen = 0;
            ByteBuffer docs = docChannel.map(FileChannel.MapMode.READ_WRITE, offsetDocs, 4L * postingList.size());
            ByteBuffer tfs = tfChannel.map(FileChannel.MapMode.READ_WRITE, offsetTfs, 4L * postingList.size());
            //iterate over the posting of the term
            for(Posting p: postingList){ //take the posting list
                //write posting list into compressed file : for each posting compress and write on appropriate file
                //compress the docid with variable byte
                /*byte[] compressedDocs = compressor.variableByteEncodeNumber(p.getDocid());
                ByteBuffer bufferValue = ByteBuffer.allocate(compressedDocs.length);
                bufferValue.put(compressedDocs);
                bufferValue.flip();
                docChannel.write(bufferValue);*/
                //compress the TermFreq with unary
                /*byte[] compressedTF = compressor.unaryEncode(p.getTf()); //compress the term frequency with unary
                ByteBuffer bufferFreq = ByteBuffer.allocate(compressedTF.length);
                bufferFreq.put(compressedTF);
                bufferFreq.flip();
                tfChannel.write(bufferFreq);*/
                docs.putInt(p.getDocid());
                tfs.putInt(p.getTf());
                docLen+= 4;
                tfLen+= 4;
            }
            //we check if the string is greater than 20 chars, in that case we truncate it
            byte[] lexiconBytes = Utils.getBytesFromString(term);
            lexiconBytes = addByteArray(lexiconBytes, Utils.createLexiconEntry(lexiconStats.getdF(), lexiconStats.getCf(), docLen, tfLen, offsetDocs, offsetTfs, 0.0, 0.0, 0.0, 0, 0));
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

