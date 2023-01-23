package queryProcessing;

import fileManager.ConfigurationParameters;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Scorer {
    private static final double k1 = 1.2;
    private static final double b = 0.75;
    private static double avg_len;

    static {
        try {
            avg_len = ConfigurationParameters.getAverageDocumentLength();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

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
    public static double bm25Weight(int tf_d, int d_len, double idf) throws IOException {
        //return (((double)tf_d/((k1*((1-b) + b * (d_len/avg_len)))+tf_d)))*Math.log(htDocindex.keySet().size()/doc_freq);
        //we already computed the logarithm for the computation of idf, so we don't apply it here!!!
        return (((double)tf_d/((k1*((1-b) + b * (d_len/avg_len)))+tf_d)))*idf;
    }

    //method for normalizing the scores obtained with bm25
    private void normalizeScores(HashMap<Integer, Double> sortedScores, double totalScore){
        for(Map.Entry<Integer,Double> e: sortedScores.entrySet()){
            sortedScores.put(e.getKey(), e.getValue()/totalScore);
        }
    }
}
