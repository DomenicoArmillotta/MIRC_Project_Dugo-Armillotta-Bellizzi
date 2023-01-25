package invertedIndex;

/**
 * used to store the compressed POSTING LIST  belonging to a posting list
 */
public class CompressedList {
    private byte[] docids;
    private byte[] tfs;
    private byte[] skipInfo;

    public CompressedList(byte[] docids, byte[] tfs, byte[] skipInfo){
        this.docids = docids;
        this.tfs = tfs;
        this.skipInfo = skipInfo;
    }
    public byte[] getDocids() {
        return docids;
    }

    public void setDocids(byte[] docids) {
        this.docids = docids;
    }

    public byte[] getTfs() {
        return tfs;
    }

    public void setTfs(byte[] tfs) {
        this.tfs = tfs;
    }

    public byte[] getSkipInfo() {
        return skipInfo;
    }

    public void setSkipInfo(byte[] skipInfo) {
        this.skipInfo = skipInfo;
    }
}
