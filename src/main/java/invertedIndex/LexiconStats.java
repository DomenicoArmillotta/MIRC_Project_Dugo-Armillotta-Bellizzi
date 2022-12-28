package invertedIndex;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class LexiconStats implements Serializable {

    private int dF; //document frequency
    private long cf; //collection frequency
    private long offsetDocid; //offset of the posting list of the term
    private long offsetTf;
    private int docidsLen;
    private int tfLen;
    private int index;
    private int curdoc;
    private int curTf;
    private double idf;

    //public List<Posting> pointer;

    //TODO: decidere se metter qua l'indice e creare le statistiche man mano in spimi

    public LexiconStats(){
        this.dF = 0;
        this.cf = 0;
        this.offsetDocid = 0;
        this.offsetTf = 0;
        this.curdoc = 0;
    }

    public LexiconStats(ByteBuffer b){
        //use the getInt/getLong method of ByteBuffer to read the value at the correct position
        this.dF = b.getInt(); //read first int
        this.cf = b.getLong(4); //read second value, a long
        this.docidsLen = b.getInt(12); //read third value, an int
        this.tfLen = b.getInt(16); //read fourth value, an int
        this.offsetDocid = b.getLong(20); //read fifth value, a long
        this.offsetTf = b.getLong(28); //read sixth value, a long
    }

    /*public LexiconStats(ByteBuffer b){
        //use the getInt/getLong method of ByteBuffer to read the value at the correct position
        this.dF = b.getInt(); //read first int
        this.cf = b.getLong(4); //read second value, a long
        this.docidsLen = b.getInt(12); //read third value, an int
        this.tfLen = b.getInt(16); //read fourth value, an int
        this.offsetDocid = b.getLong(20); //read fifth value, a long
        this.offsetTf = b.getLong(28); //read sixth value, a long
        this.idf = b.getDouble(36); //read seventh value, a double
    }*/

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
