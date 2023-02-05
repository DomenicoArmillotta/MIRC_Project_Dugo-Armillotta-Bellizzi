package invertedIndex;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Information with which each term in the lexicon is associated
 */
public class LexiconStats implements Serializable {

    private int dF; //document frequency
    private long cf; //collection frequency
    private long offsetDocid; //offset of the docIds posting list of the term
    private long offsetTf; //offset of the term frequencies posting list
    private int docidsLen; //length of the docIds posting list
    private int tfLen; //length of the term frequencies posting list
    private int index; //used to map a list to the term
    private int curdoc; //used to keep the current docID during processing
    private int curTf; //used to keep the current Tf during processing
    private double idf; //the idf of the term
    private double termUpperBound; //the term upperbound computed with bm25
    private double termUpperBoundTfIdf; //the term upperbound computed with tfidf
    private long offsetSkip; //offset of the skipping information for this term
    private int skipLen; //length of the skipping information for this term

    public LexiconStats(){
        this.dF = 0;
        this.cf = 0;
        this.offsetDocid = 0;
        this.offsetTf = 0;
        this.curdoc = 0;
    }

    public LexiconStats(ByteBuffer b){
        //use the getInt/getLong/getDouble method of ByteBuffer to read the value at the correct position
        this.dF = b.getInt(); //read first int
        this.cf = b.getLong(4); //read second value, a long
        this.docidsLen = b.getInt(12); //read third value, an int
        this.tfLen = b.getInt(16); //read fourth value, an int
        this.offsetDocid = b.getLong(20); //read fifth value, a long
        this.offsetTf = b.getLong(28); //read sixth value, a long
        this.idf = b.getDouble(36); //read seventh value, a double
        this.termUpperBound = b.getDouble(44);
        this.termUpperBoundTfIdf = b.getDouble(52);
        this.offsetSkip = b.getLong(60);
        this.skipLen = b.getInt(68);
    }

    //copy constructor
    public LexiconStats(LexiconStats lexiconStats){
        this.dF = lexiconStats.getdF();
        this.cf =lexiconStats.getCf();
        this.docidsLen = lexiconStats.getDocidsLen();
        this.tfLen = lexiconStats.getTfLen();
        this.offsetDocid = lexiconStats.getOffsetDocid();
        this.offsetTf = lexiconStats.getOffsetTf();
        this.idf = lexiconStats.getIdf();
        this.termUpperBound = lexiconStats.getTermUpperBound();
        this.termUpperBoundTfIdf = lexiconStats.getTermUpperBoundTfIdf();
        this.offsetSkip = lexiconStats.getOffsetSkip();
        this.skipLen = lexiconStats.getSkipLen();
        this.curdoc = 0;
        this.curTf = 0;
        this.index = 0;
    }

    public int getdF() {
        return dF;
    }

    public void setdF(int dF) {
        this.dF = dF;
    }

    public long getCf() {
        return cf;
    }

    public void setCf(long cf) {
        this.cf = cf;
    }

    public long getOffsetDocid() {
        return offsetDocid;
    }

    public void setOffsetDocid(long offset) {
        this.offsetDocid = offset;
    }

    public long getOffsetTf() {
        return offsetTf;
    }

    public void setOffsetTf(long offsetTf) {
        this.offsetTf = offsetTf;
    }

    public int getDocidsLen() {
        return docidsLen;
    }

    public void setDocidsLen(int docidsLen) {
        this.docidsLen = docidsLen;
    }

    public int getTfLen() {
        return tfLen;
    }

    public void setTfLen(int tfLen) {
        this.tfLen = tfLen;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }


    public int getCurdoc() {
        return curdoc;
    }

    public void setCurdoc(int curdoc) {
        this.curdoc = curdoc;
    }

    public int getCurTf() {
        return curTf;
    }

    public void setCurTf(int curTf) {
        this.curTf = curTf;
    }

    public double getIdf() {
        return idf;
    }

    public void setIdf(double idf) {
        this.idf = idf;
    }

    public double getTermUpperBound() {
        return termUpperBound;
    }

    public void setTermUpperBound(double termUpperBound) {
        this.termUpperBound = termUpperBound;
    }

    public double getTermUpperBoundTfIdf() { return termUpperBoundTfIdf;}

    public void setTermUpperBoundTfIdf(double termUpperBoundTfIdf) { this.termUpperBoundTfIdf = termUpperBoundTfIdf; }

    public long getOffsetSkip() {
        return offsetSkip;
    }

    public void setOffsetSkip(long offsetSkip) {
        this.offsetSkip = offsetSkip;
    }

    public int getSkipLen() {
        return skipLen;
    }

    public void setSkipLen(int skipLen) {
        this.skipLen = skipLen;
    }


    private void writeObject(ObjectOutputStream oos)
            throws IOException {
        oos.defaultWriteObject();
        oos.writeInt(this.dF);
        oos.writeLong(this.cf);
        oos.writeLong(this.offsetDocid);
        oos.writeLong(this.offsetTf);
    }

    private void readObject(ObjectInputStream ois)
            throws ClassNotFoundException, IOException {
        ois.defaultReadObject();
        this.dF = ois.readInt();
        this.cf = ois.readLong();
        this.offsetDocid = ois.readLong();
        this.offsetTf = ois.readLong();
    }

}
