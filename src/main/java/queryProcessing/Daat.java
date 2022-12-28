package queryProcessing;

import invertedIndex.LexiconStats;
import invertedIndex.Posting;
import preprocessing.PreprocessDoc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;


public class Daat {

    private int maxDocID;

    private HashMap<String, LexiconStats> lexicon;
    private HashMap<String, Integer> docIndex;


    public Daat(){
    }

    public void conjunctiveDaat(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = new ArrayList<>();
        proQuery = preprocessing.preprocess_doc_optimized(query);
        int queryLen = proQuery.size();
        //TODO: complete

    }

    public void disjunctiveDaat(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = new ArrayList<>();
        proQuery = preprocessing.preprocess_doc_optimized(query);
        int queryLen = proQuery.size();
        //TODO: complete --> bisogna implementare maxscore:
        // vedere se fare term upper bound

    }



    public ArrayList<Posting> openList(String queryTerm) throws IOException {
        //TODO: implement
        return null;
    }

    //iterate over the posting list ot get the desired term frequency, return 0 otherwise
    private int getFreq(ArrayList<Posting> postingList, int docid){
        //TODO: implement
        // bisogna leggere la lista usando i puntatori e prendere la tf corrispondente al docid:
        // conviene utilizzare un puntatore
        return 0;
    }
    /*private int getFreq(ArrayList<Posting> postingList, int docid){
        for(Posting p: postingList){
            if(p.getDocumentId() == docid) return p.getTermFrequency();
        }
        return 0;
    }*/

    private int next(RandomAccessFile file, String term, int value) throws IOException {
        //TODO: add a pointer of the last document processed
        //Seek to the position in the file where the posting list for the term is stored
        file.seek(lexicon.get(term).getOffsetDocid());

        // Read the compressed posting list data from the file
        byte[] data = new byte[lexicon.get(term).getDocidsLen()];
        file.read(data);
        // Decompress the data using the appropriate decompression algorithm
        //List<Integer> posting_list = decompress(data);

        // Iterate through the posting list and return the next entry in the list
        /*for (int doc_id : posting_list) {
            return doc_id;
        }*/


        // If no such value was found, return a special value indicating that the search failed
        return -1;
    }

    //nextGEQ(lp, k) find the next posting in list lp with docID >= k and
    //return its docID. Return value > MAXDID if none exists.
    private int nextGEQ(RandomAccessFile file, String term, int value) throws IOException {
        //TODO: implement
        //Seek to the position in the file where the posting list for the term is stored
        file.seek(lexicon.get(term).getOffsetDocid());

        // Read the compressed posting list data from the file
        byte[] data = new byte[lexicon.get(term).getDocidsLen()];
        file.read(data);

        // Decompress the data using the appropriate decompression algorithm
        //List<Integer> posting_list = decompress(data);

        // Iterate through the posting list and return the first entry that is greater than or equal to the search value
        /*for (int doc_id : posting_list) {
            if (doc_id >= value) {
                return doc_id;
            }
        }*/

        // If no such value was found, return a special value indicating that the search failed
        return -1;
    }

    //computes the average document length over the whole collection
    /*private double averageDocumentLength(){
        double avg = 0;
        for(int len: htDocindex.values()){
            avg+= len;
        }
        return avg/ htDocindex.keySet().size();
    }*/

}
