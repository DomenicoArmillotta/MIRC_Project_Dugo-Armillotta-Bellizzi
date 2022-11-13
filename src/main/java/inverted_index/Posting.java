package inverted_index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Posting implements Comparable<Posting>, Serializable {
    private int docid;
    private int termfreq;
    private List<Integer> pos = new ArrayList<>();

    public Posting(int docid, int termfreq) {
        this.docid = docid;
        this.termfreq = termfreq;
    }

    public Posting(int docid, int termfreq, int pos) {
        this.docid = docid;
        this.termfreq = termfreq;
        this.pos.add(pos);
    }
    public Posting(int docid, int termfreq, List<Integer> pos) {
        this.docid = docid;
        this.termfreq = termfreq;
        this.pos = pos;
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

    public List getPos() {
        return pos;
    }

    public String getPositionString(){
        String position = "";
        for(int i = 0; i < pos.size(); i++){
            position += i == pos.size()-1 ? pos.get(i) : pos.get(i) + "-"; //to avoid conflicting with toString()
        }
        return position;
    }

    public void addPos(int pos) {
        this.pos.add(pos);
    }

    public void setPos(List pos) {
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
