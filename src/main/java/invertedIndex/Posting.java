package invertedIndex;

import java.io.Serializable;

/**
 * Posting is used to build and manipulate posting list in the inverted index structure
 */
public class Posting implements Comparable<Posting>, Serializable {
    private int docid;
    //term frequency
    private int tf;

    public Posting(int docid, int tf){
        this.docid = docid;
        this.tf = tf;
    }

    public int getDocid() {

        return docid;
    }

    public void setDocid(int docid) {
        this.docid = docid;
    }

    public int getTf() {
        return tf;
    }

    public void setTf(int tf) {
        this.tf = tf;
    }

    public String toString() {
        return String.format("%d,%d",this.docid, this.tf);
    }

    public int compareTo(Posting o) {
        return this.docid - o.getDocid();
    }
}
