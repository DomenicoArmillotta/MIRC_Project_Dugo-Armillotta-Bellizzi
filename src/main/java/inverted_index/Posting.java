package inverted_index;

public class Posting {
    private int docid;
    private int termfreq;

    public Posting(int docid, int termfreq) {
        this.docid = docid;
        this.termfreq = termfreq;
    }

    public long getDocumentId() {
        return docid;
    }

    public int getTermFrequency() {
        return termfreq;
    }

    public void addOccurrence() {
        this.termfreq++;
    }

    public String toString() {
        return String.format("%d,%d", this.docid, this.termfreq);
    }
}
