package queryProcessing;


public class ScoreEntry implements Comparable<ScoreEntry> {

    private int docID;

    private double score;

    public ScoreEntry(int docID, double score){
        this.docID = docID;
        this.score = score;
    }

    public int getDocID() {
        return docID;
    }

    public void setDocID(int docID) {
        this.docID = docID;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    @Override
    public int compareTo(ScoreEntry o) {
        return Double.compare(this.getScore(), o.getScore());
    }

    @Override
    public String toString() {
        return "[docid=" + docID + ", score=" + score + "]";
    }
}
