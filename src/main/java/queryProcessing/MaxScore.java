package queryProcessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MaxScore {

    public static double getMaxScoreBM25(double maxscore, int tf, int docLen, double idf) throws IOException {
        double score = Scorer.bm25Weight(tf, docLen, idf);
        return score>maxscore ? score : maxscore;
    }
    public static double getMaxScoreTFIDF(double maxscore, int tf, double idf) throws IOException {
        double score = Scorer.tfidf(tf, idf);
        return score>maxscore ? score : maxscore;
    }
}
