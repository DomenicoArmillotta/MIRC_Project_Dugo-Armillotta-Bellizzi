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

    /*public Posting(byte[] docidb, byte[] tfb){
        this.docidb = ByteBuffer.allocate(4).putInt(ByteBuffer.wrap(docidb).getInt()).array();
        this.tfb = ByteBuffer.allocate(4).putInt(ByteBuffer.wrap(tfb).getInt()).array();
    }*/

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

    /*public byte[] getDocidb() {
        return docidb;
    }
    public byte[] getTfb() {
        return tfb;
    }

    public void setTfb(byte[] tfb) {
        this.tfb = tfb;
    }*/

    /*public String toString() {
        return String.format("%d,%d", ByteBuffer.wrap(this.docidb).getInt(), ByteBuffer.wrap(this.tfb).getInt());
    }*/

    public String toString() {
        return String.format("%d,%d",this.docid, this.tf);
    }
   /* @Override
    public int compareTo(Posting o) {
        return ByteBuffer.wrap(this.docidb).getInt() - ByteBuffer.wrap(o.getDocidb()).getInt();
    }*/

    public int compareTo(Posting o) {
        return this.docid - o.getDocid();
    }

    //non serve a niente abbiamo già la get
    /*public byte[] getDoc() {
        // Allocate a byte buffer with 4 bytes and put the docid into it
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put(this.docid);
        // Return the array of bytes
        return buffer.array();
    }*/



    /*private void writeObject(ObjectOutputStream oos)
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
    }*/
}
