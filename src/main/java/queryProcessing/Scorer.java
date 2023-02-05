package queryProcessing;

import fileManager.ConfigurationParameters;
import java.io.IOException;

/**
 * class to compute tfidf and bm25 weights
 */
public class Scorer {
    private static final double k1 = 1.2;
    private static final double b = 0.75;
    private static double avgLen;

    //avg len taken from configParameters.txt
    static {
        try {
            avgLen = ConfigurationParameters.getAverageDocumentLength();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * tfidf scoring function for computing term frequency weights
     * @param tf
     * @param idf
     * @return tfidf
     */
    public static double tfidf(int tf, double idf){
        return (1.0 + Math.log(tf)*idf);
    }


    /**
     * bm25 scoring function for computing weights for term frequency
     * @param tf
     * @param docLen
     * @param idf
     * @return bm25 weights
     * @throws IOException
     */
    public static double bm25Weight(int tf, int docLen, double idf) throws IOException {
        //we already computed the logarithm for the computation of idf, so we don't apply it here!!!
        return (((double)tf/((k1*((1-b) + b * (docLen/ avgLen)))+tf)))*idf;
    }
}
