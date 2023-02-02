package queryProcessing;

import fileManager.ConfigurationParameters;

import java.io.IOException;

public class Scorer {
    private static final double k1 = 1.2;
    private static final double b = 0.75;
    private static double avgLen;

    static {
        try {
            avgLen = ConfigurationParameters.getAverageDocumentLength();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //tfidf scoring function for computing term frequency weights
    public static double tfidf(int tf, double idf){
        return (1.0 + Math.log(tf)*idf);
    }

    //normalized version of tfidf
    private double tfidfNorm(int tf, int docLen, int docFreq, double idf){
        return (1.0 + Math.log(tf)*Math.log(idf))/(double)docLen;
    }

    //bm25 scoring function for computing weights for term frequency
    public static double bm25Weight(int tf, int docLen, double idf) throws IOException {
        //we already computed the logarithm for the computation of idf, so we don't apply it here!!!
        return (((double)tf/((k1*((1-b) + b * (docLen/ avgLen)))+tf)))*idf;
    }
}
