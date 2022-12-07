package queryProcessing;

import invertedIndex.Posting;
import preprocessing.PreprocessDoc;

import java.io.IOException;
import java.util.*;


public class Daat {

    private final double k1 = 1.2;
    private final double b = 0.75;

    private int maxDocID;


    public Daat(){
    }

    //TODO 30/10/2022: da controllare se va bene
    public void conjunctiveDaat(String query_string, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> pro_query = new ArrayList<>();
        pro_query = preprocessing.preprocess_doc_optimized(query_string);
        int query_len = pro_query.size();
        //TODO: complete

    }

    public void disjunctiveDaat(String query_string, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> pro_query = new ArrayList<>();
        pro_query = preprocessing.preprocess_doc_optimized(query_string);
        int query_len = pro_query.size();
        //TODO: complete

    }



    public ArrayList<Posting> openList(String query_string) throws IOException {
        //TODO: implement
        return null;
    }

    //iterate over the posting list ot get the desired term frequency, return 0 otherwise
    /*private int getFreq(ArrayList<Posting> postingList, int docid){
        for(Posting p: postingList){
            if(p.getDocumentId() == docid) return p.getTermFrequency();
        }
        return 0;
    }*/

    private int next(Iterator<Integer> it){
        //TODO: implement
        return 0;
    }

    //nextGEQ(lp, k) find the next posting in list lp with docID >= k and
    //return its docID. Return value > MAXDID if none exists.
    private int nextGEQ(LinkedList<Posting> invertedLists, int prev) {
        //TODO: implement
        return 0;
    }

    //computes the average document length over the whole collection
    /*private double averageDocumentLength(){
        double avg = 0;
        for(int len: htDocindex.values()){
            avg+= len;
        }
        return avg/ htDocindex.keySet().size();
    }*/

    //tfidf scoring function for computing term frequency weights
    private double tfidf(int tf_d, int d_len, int doc_freq){
        double idf = 1.0; //da inizializzare
        //return (1.0 + Math.log(tf_d)*Math.log(htDocindex.keySet().size()/doc_freq));
        return (1.0 + Math.log(tf_d)*Math.log(idf));
    }

    //normalized version of tfidf
    private double tfidfNorm(int tf_d, int d_len, int doc_freq){
        double idf = 1.0; //da inizializzare
        //return (1.0 + Math.log(tf_d)*Math.log(htDocindex.keySet().size()/doc_freq))/(double)d_len;
        return (1.0 + Math.log(tf_d)*Math.log(idf))/(double)d_len;
    }

    //bm25 scoring function for computing weights for term frequency
    private double bm25Weight(int tf_d, int d_len, int doc_freq, double avg_len){
        double idf = 1.0; //da inizializzare
        //return (((double)tf_d/((k1*((1-b) + b * (d_len/avg_len)))+tf_d)))*Math.log(htDocindex.keySet().size()/doc_freq);
        return (((double)tf_d/((k1*((1-b) + b * (d_len/avg_len)))+tf_d)))*Math.log(idf);
    }

    //method for normalizing the scores obtained with bm25
    private void normalizeScores(HashMap<Integer, Double> sortedScores, double totalScore){
        for(Map.Entry<Integer,Double> e: sortedScores.entrySet()){
            sortedScores.put(e.getKey(), e.getValue()/totalScore);
        }
    }
}
