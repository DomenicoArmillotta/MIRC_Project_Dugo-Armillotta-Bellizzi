package invertedIndex;

import java.io.Serializable;
import java.nio.ByteBuffer;

public class Posting implements Comparable<Posting>, Serializable {

    private byte[] docid;

    private byte[] tf;

    public Posting(byte[] docidb, byte[] tfb){
        this.docid = ByteBuffer.allocate(4).putInt(ByteBuffer.wrap(docidb).getInt()).array();
        this.tf = ByteBuffer.allocate(4).putInt(ByteBuffer.wrap(tfb).getInt()).array();
    }

    public byte[] getDocid() {
        return docid;
    }

    public byte[] getTf() {
        return tf;
    }

    public void setTf(byte[] tf) {
        this.tf = tf;
    }

    public String toString() {
        return String.format("%d,%d", ByteBuffer.wrap(this.docid).getInt(), ByteBuffer.wrap(this.tf).getInt());
    }

    @Override
    public int compareTo(Posting o) {
        return ByteBuffer.wrap(this.docid).getInt() - ByteBuffer.wrap(o.getDocid()).getInt();
    }

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
