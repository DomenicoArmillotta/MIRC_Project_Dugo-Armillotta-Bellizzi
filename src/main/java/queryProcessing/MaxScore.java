package queryProcessing;
import java.io.IOException;

/**
 * used to compute TFIDF and BM25 max score
 */
public class MaxScore {

    /**
     * compute BM25  max score
     * @param maxscore
     * @param tf term frequencies
     * @param docLen document lenght
     * @param idf
     * @return score if > maxScore
     * @throws IOException
     */
    public static double getMaxScoreBM25(double maxscore, int tf, int docLen, double idf) throws IOException {
        double score = Scorer.bm25Weight(tf, docLen, idf);
        return score>maxscore ? score : maxscore;
    }

    /**
     * compute TFIDF max score
     * @param maxscore
     * @param tf term frequencies
     * @param idf
     * @return score if > maxScore
     * @throws IOException
     */
    public static double getMaxScoreTFIDF(double maxscore, int tf, double idf) throws IOException {
        double score = Scorer.tfidf(tf, idf);
        return score>maxscore ? score : maxscore;
    }
}
