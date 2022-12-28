package queryProcessing;

import java.util.HashMap;
import java.util.Map;

public class Scorer {
    private final double k1 = 1.2;
    private final double b = 0.75;

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
