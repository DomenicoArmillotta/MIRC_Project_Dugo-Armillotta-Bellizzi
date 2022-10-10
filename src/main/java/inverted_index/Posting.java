package inverted_index;

public class Posting {
    private int id;
    private int termfreq;

    public Posting(int id, int termfreq) {
        this.id = id;
        this.termfreq = termfreq;
    }

    public long getDocumentId() {
        return id;
    }

    public int getTermFrequency() {
        return termfreq;
    }

    public void addOccurrence() {
        this.termfreq++;
    }

    public String toString() {
        return String.format("%d,%d", this.id, this.termfreq);
    }
}
