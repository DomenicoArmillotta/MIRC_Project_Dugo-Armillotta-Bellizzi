package invertedIndex;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class Posting implements Comparable<Posting>, Serializable {
    private int docid;
    private int tf;

    public Posting(int docid, int tf){
        this.docid = docid;
        this.tf = tf;
    }
    private byte[] docidb;
    private byte[] tfb;
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
