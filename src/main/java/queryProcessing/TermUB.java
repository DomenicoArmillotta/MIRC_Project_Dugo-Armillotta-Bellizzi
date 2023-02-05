package queryProcessing;

/**
 * used in the query processing to build the priority queue
 */
public class TermUB implements Comparable<TermUB>{

    private String term;

    private double maxScore;

    public TermUB(String term, double score){
        this.term = term;
        this.maxScore = score;
    }

    public double getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(double maxScore) {
        this.maxScore = maxScore;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }


    @Override
    public int compareTo(TermUB o) {
        return Double.compare(this.getMaxScore(), o.getMaxScore());
    }

    @Override
    public String toString() {
        return "[term=" + term + ", score=" + maxScore + "]";
    }

}
