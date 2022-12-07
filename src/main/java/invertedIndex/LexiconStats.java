package invertedIndex;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class LexiconStats implements Serializable {

    private int dF; //document frequency
    private long cf; //collection frequency
    private long offsetDocid; //offset of the posting list of the term
    private long offsetTf;
    private int docidsLen;
    private int tfLen;

    private int index;

    //TODO: decidere se metter qua l'indice e creare le statistiche man mano in spimi

    public LexiconStats(){
        this.dF = 0;
        this.cf = 0;
        this.offsetDocid = 0;
        this.offsetTf = 0;
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
