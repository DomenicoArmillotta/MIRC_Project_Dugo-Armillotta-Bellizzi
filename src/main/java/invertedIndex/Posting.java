package invertedIndex;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Posting implements Comparable<Posting>, Serializable {
    private int docid;
    private int termfreq;

    public Posting(int docid, int termfreq) {
        this.docid = docid;
        this.termfreq = termfreq;
    }

    public int getDocumentId() {
        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public void setTermfreq(int termfreq) {
        this.termfreq = termfreq;
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

    @Override
    public int compareTo(Posting o) {
        return this.docid - o.getDocumentId();
    }

    private void writeObject(ObjectOutputStream oos)
            throws IOException {
        oos.defaultWriteObject();
        oos.writeInt(this.docid);
        oos.writeInt(this.termfreq);
    }

    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        this.docid = ois.readInt();
        this.termfreq = ois.readInt();
    }
}
