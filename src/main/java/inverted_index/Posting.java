package inverted_index;

public class Posting implements Comparable<Posting>{
    private int docid;
    private int termfreq;
    private int pos;

    public Posting(int docid, int termfreq, int pos) {
        this.docid = docid;
        this.termfreq = termfreq;
        this.pos = pos;
    }

    public int getDocumentId() {
        return docid;
    }

    public int getTermFrequency() {
        return termfreq;
    }

    public void addOccurrence() {
        this.termfreq++;
    }

    public int getPos() {
        return pos;
    }

    public void setPos(int pos) {
        this.pos = pos;
    }

    public String toString() {
        return String.format("%d,%d", this.docid, this.termfreq);
    }

    @Override
    public int compareTo(Posting o) {
        return this.docid - o.getDocumentId();
    }
}
