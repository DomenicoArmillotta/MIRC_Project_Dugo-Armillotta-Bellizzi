package queryProcessing;

import compression.Compressor;
import fileManager.ConfigurationParameters;
import indexing.DocumentIndex;
import invertedIndex.LexiconStats;
import preprocessing.PreprocessDoc;
import utility.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;


public class Daat {

    private int maxDocID;

    public HashMap<String, LexiconStats> lexicon;
    private HashMap<Integer,Integer> docIndex;
    private int[] numPosting;
    private int[] endDocids;
    private Iterator<Integer>[] docIdsIt;
    private Iterator<Integer>[] tfsIt;
    private String lexiconPath = "docs/lexicon.txt";
    private String docidsPath = "docs/docids.txt";
    private String tfsPath = "docs/tfs.txt";
    private String docIndexPath = "docs/docIndex.txt";
    private String skipsPath = "docs/skipInfo.txt";
    private List<Integer>[] decompressedDocIds; //to keep the current block of docids for each term of the query
    private List<Integer>[] decompressedTfs; //to keep the current block of tfs for each term of the query
    private FileChannel lexChannel;
    private FileChannel docChannel;
    private FileChannel tfChannel;
    private FileChannel skipChannel;
    private FileChannel docIndexChannel;

    public Daat() throws IOException {
        lexicon = new HashMap<>();
        maxDocID = (int)ConfigurationParameters.getNumberOfDocuments();
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        lexChannel = lexFile.getChannel();
        RandomAccessFile docFile = new RandomAccessFile(new File(docidsPath), "rw");
        docChannel = docFile.getChannel();
        RandomAccessFile tfFile = new RandomAccessFile(new File(tfsPath), "rw");
        tfChannel = tfFile.getChannel();
        RandomAccessFile skipFile = new RandomAccessFile(new File(skipsPath), "rw");
        skipChannel = skipFile.getChannel();
        RandomAccessFile docIndexFile = new RandomAccessFile(new File(docIndexPath), "rw");
        docIndexChannel = docIndexFile.getChannel();
        DocumentIndex d = new DocumentIndex();
        docIndex = d.getDocIndex();
    }


    public List<ScoreEntry> conjunctiveDaat(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = preprocessing.preprocess_doc(query);
        int queryLen = proQuery.size();
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numPosting = new int[queryLen];
        endDocids = new int[queryLen];
        docIdsIt = new Iterator[queryLen];
        tfsIt = new Iterator[queryLen];
        PriorityQueue<ScoreEntry> scores = new PriorityQueue<>(); //to store partial scores results
        for(String term: proQuery){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        String[] queryTerms = new String[queryLen];
        for(int i = 0; i < queryLen; i++){
            String term = proQuery.get(i);
            lexicon.get(term).setIndex(i);
            lexicon.get(term).setCurdoc(0);
            queryTerms[i] = term;
            openList(docChannel, tfChannel, skipChannel, queryTerms[i]);
        }
        //TODO: sort query terms in lexicon by shortest list
        int did = getMinDocid(queryTerms);
        while (did <= maxDocID){
            double score = 0.0;
            did = nextGEQ(proQuery.get(0), did);
            if(did == maxDocID){
                break;
            }
            int d = 0;
            for (int i=1; (i<queryLen) && ((d=nextGEQ(proQuery.get(i), did)) == did); i++);
            if (d > did){
                did = d; // not in intersection
            }
            else {
                //docID is in intersection; now get all frequencies
                for (int i=0; i<proQuery.size(); i++){
                    int tf = lexicon.get(proQuery.get(i)).getCurTf();
                    int docLen = docIndex.get(did);
                    double idf = lexicon.get(proQuery.get(i)).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.bm25Weight(tf, docLen, idf); //to modify after getFreq
                }
                scores.add(new ScoreEntry(did, score));
                if(scores.size() > k){
                    scores.poll(); //remove the minimum element
                }
                did++; //and increase did to search for next post
            }
        }
        // Return the result list
        return scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList());
    }

    public List<ScoreEntry> disjunctiveDaat(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = preprocessing.preprocess_doc(query);
        int queryLen = proQuery.size();
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numPosting = new int[queryLen];
        endDocids = new int[queryLen];
        docIdsIt = new Iterator[queryLen];
        tfsIt = new Iterator[queryLen];
        PriorityQueue<ScoreEntry> scores = new PriorityQueue<>(); //to store partial scores results
        double[] termUB = new double[queryLen];
        for(String term: proQuery){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        for(int i = 0; i < queryLen; i++){
            termUB[i] = lexicon.get(proQuery.get(i)).getTermUpperBound();
        }
        Arrays.sort(termUB);
        String [] queryTerms = new String[queryLen];
        for(String term: proQuery){
            double ub = lexicon.get(term).getTermUpperBound();
            int i = Arrays.binarySearch(termUB, ub);
            queryTerms[i] = term;
        }
        for(int i = 0; i < queryLen; i++){
            lexicon.get(queryTerms[i]).setIndex(i);
            lexicon.get(queryTerms[i]).setCurdoc(0);
            openList(docChannel, tfChannel, skipChannel, queryTerms[i]);
        }
        int pivot = 0;
        double[] documentUB = new double[queryLen];
        double prec = 0.0;
        int index = 0;
        for(double maxScore: termUB){
            documentUB[index] = maxScore + prec;
            prec = documentUB[index];
            index++;
        }
        double threshold = 0.0;
        int next;
        int did = getMinDocid(queryTerms);
        while (pivot < queryLen && did != maxDocID){
            next = maxDocID;
            double score = 0.0;
            //process essential lists
            for (int i=pivot; i<queryLen; i++){
                int current = nextGEQ(queryTerms[i], did);
                if(current == did){
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    int docLen = docIndex.get(did);
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.bm25Weight(tf, docLen, idf);
                    current = nextGEQ(queryTerms[i], did+1); //update the pointer to next docid
                }
                if((current < next)){
                    next = current;
                }
            }
            //process non essential lists
            for (int i=pivot-1; i>=0; i--){
                //check document upper bound
                if(documentUB[i] + score <= threshold){
                    break;
                }
                int current = nextGEQ(queryTerms[i], did);
                if(current == did) {
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    int docLen = docIndex.get(did);
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.bm25Weight(tf, docLen, idf);
                }
            }
            //update pivot
            //check if the new threshold is higher than previous one, in this case update the threshold
            scores.add(new ScoreEntry(did, score));
            if(scores.size() > k){
                scores.poll(); //remove the minimum element
            }
            double min = scores.peek().getScore();
            if(scores.size() == k && min > threshold) {
                threshold = min;
                while (pivot < queryLen && documentUB[pivot] <= threshold) {
                    pivot++;
                }
            }
            did = next;
        }
        return scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList());
    }

    private int getMinDocid(String[] queryTerms) throws IOException {
        int did = nextGEQ(queryTerms[0], 1);
        for (int i=1; (i<queryTerms.length); i++){
            int d= nextGEQ(queryTerms[i], did);
            if(d<did){
                did = d;
            }
            docIdsIt[lexicon.get(queryTerms[i]).getIndex()] = decompressedDocIds[lexicon.get(queryTerms[i]).getIndex()].iterator();
            tfsIt[lexicon.get(queryTerms[i]).getIndex()] = decompressedTfs[lexicon.get(queryTerms[i]).getIndex()].iterator();
        }
        docIdsIt[lexicon.get(queryTerms[0]).getIndex()] = decompressedDocIds[lexicon.get(queryTerms[0]).getIndex()].iterator();
        tfsIt[lexicon.get(queryTerms[0]).getIndex()] = decompressedTfs[lexicon.get(queryTerms[0]).getIndex()].iterator();
        return did;
    }

    //TODO: fare trec eval!!!!!

    private int nextGEQ(String term, int value) throws IOException {
        Iterator<Integer> itDocs = docIdsIt[lexicon.get(term).getIndex()];
        Iterator<Integer> itTfs = tfsIt[lexicon.get(term).getIndex()];
        //System.out.println("NextGEQ: " + term + " " + value + " " + lexicon.get(term).getCurdoc() + " " + endDocids[lexicon.get(term).getIndex()]);
        while(lexicon.get(term).getCurdoc() <= endDocids[lexicon.get(term).getIndex()]) { //we need to update the index; check if we are in the last block
            int prec = lexicon.get(term).getCurdoc();
            if(prec >= value) {
                return prec;
            }
            int docId = prec;
            int tf = lexicon.get(term).getCurTf();
            if(itDocs.hasNext() && itTfs.hasNext()) {
                docId = itDocs.next();
                tf = itTfs.next();
            }
            lexicon.get(term).setCurdoc(docId);
            lexicon.get(term).setCurTf(tf);
            docIdsIt[lexicon.get(term).getIndex()] = itDocs; //update the iterator (non so se serve)
            tfsIt[lexicon.get(term).getIndex()] = itTfs;
            //check if we are in a new block
            if(value >= endDocids[lexicon.get(term).getIndex()] || docId == prec){
                //System.out.println("BLOCK ENDED!" + value + " " + docId + " " + prec);
                if(numPosting[lexicon.get(term).getIndex()]+ ConfigurationParameters.SKIP_BLOCK_SIZE >lexicon.get(term).getSkipLen()){
                    //System.out.println("END IN LOOP");
                    return maxDocID;
                }
                openList(docChannel, tfChannel, skipChannel, term);
                itDocs = docIdsIt[lexicon.get(term).getIndex()];
                itTfs = tfsIt[lexicon.get(term).getIndex()];
                //docId = itDocs.next();
                //tf = itTfs.next();
            }
        }
        // If no such value was found, return a special value indicating that the search failed
        return maxDocID;
    }
    public void openList(FileChannel docChannel, FileChannel tfChannel, FileChannel skips, String term) throws IOException {
        // Read the posting list block data from the file
        Compressor compressor = new Compressor();
        skips.position(lexicon.get(term).getOffsetSkip() + numPosting[lexicon.get(term).getIndex()]);
        ByteBuffer skipInfo = ByteBuffer.allocate(lexicon.get(term).getSkipLen() - numPosting[lexicon.get(term).getIndex()]);
        skips.read(skipInfo);
        skipInfo.position(0);
        int endocid = skipInfo.getInt();
        int skipdocid = skipInfo.getInt();
        int skiptf = skipInfo.getInt();
        endDocids[lexicon.get(term).getIndex()] = endocid;
        ByteBuffer docIds = ByteBuffer.allocate(skipdocid);
        docChannel.position(lexicon.get(term).getOffsetDocid());
        docChannel.read(docIds);
        tfChannel.position(lexicon.get(term).getOffsetTf());
        // Read the compressed posting list data from the file
        ByteBuffer tfs = ByteBuffer.allocate(skiptf);
        tfChannel.read(tfs);
        docIds.position(0);
        tfs.position(0);
        //uncompress the blocks from the disk
        int n = lexicon.get(term).getdF();
        decompressedDocIds[lexicon.get(term).getIndex()] = compressor.variableByteDecodeBlock(docIds.array(),n);
        decompressedTfs[lexicon.get(term).getIndex()] = compressor.unaryDecodeBlock(tfs.array(),n);
        //update the skip blocks read so far
        numPosting[lexicon.get(term).getIndex()] += ConfigurationParameters.SKIP_BLOCK_SIZE;
        //instantiate the iterators
        docIdsIt[lexicon.get(term).getIndex()] = decompressedDocIds[lexicon.get(term).getIndex()].iterator();
        tfsIt[lexicon.get(term).getIndex()] = decompressedTfs[lexicon.get(term).getIndex()].iterator();
        //update offsets
        lexicon.get(term).setOffsetDocid(lexicon.get(term).getOffsetDocid()+skipdocid);
        lexicon.get(term).setOffsetTf(lexicon.get(term).getOffsetTf()+skiptf);
    }
}
