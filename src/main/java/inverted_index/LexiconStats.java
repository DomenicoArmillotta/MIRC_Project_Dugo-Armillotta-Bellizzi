package inverted_index;

public class LexiconStats {

    private int dF; //document frequency
    private long cf; //collection frequency
    private long offset; //offset of the posting list of the term

    public LexiconStats(){
        this.dF = 0;
        this.cf = 0;
        this.offset = 0;
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

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

}
