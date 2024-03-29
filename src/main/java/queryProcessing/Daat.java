package queryProcessing;

import compression.Compressor;
import fileManager.ConfigurationParameters;
import indexing.DocumentIndex;
import invertedIndex.LexiconStats;
import org.junit.platform.commons.util.LruCache;
import preprocessing.Preprocessor;
import utility.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;


public class Daat {

    private int maxDocID; //keep the upper bound for the docIds
    private HashMap<String, LexiconStats> lexicon; //map the terms of the query to the pointers of the lists
    private HashMap<Integer,Integer> docIndex; //to map in memory the document index
    private LruCache<String,LexiconStats> cacheTerms = new LruCache<>(5000); //to cache the terms and pointers
    private int[] numBlocks; // counts for each term how much of the blocks have been processed
    private int[] endDocids; //store the current end docIds of the current blocks for each term
    private Iterator<Integer>[] docIdsIt; //iterators over the docIds block list
    private Iterator<Integer>[] tfsIt; //iterators over the tf block lists
    private String lexiconPath = "docs/lexicon.txt";
    private String docidsPath = "docs/docids.txt";
    private String tfsPath = "docs/tfs.txt";
    private String skipsPath = "docs/skipInfo.txt";
    private List<Integer>[] decompressedDocIds; //to keep the current block of docids for each term of the query
    private List<Integer>[] decompressedTfs; //to keep the current block of tfs for each term of the query
    private FileChannel lexChannel;
    private FileChannel docChannel;
    private FileChannel tfChannel;
    private FileChannel skipChannel;
    private Preprocessor preprocessing;

    /**
     * constructor of the query processing algorithm: initializes the file channels and the data structures required for query processing
     * @throws IOException
     */
    public Daat() throws IOException {
        preprocessing = new Preprocessor();
        maxDocID = (int)ConfigurationParameters.getNumberOfDocuments() + 1;
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        lexChannel = lexFile.getChannel();
        RandomAccessFile docFile = new RandomAccessFile(new File(docidsPath), "rw");
        docChannel = docFile.getChannel();
        RandomAccessFile tfFile = new RandomAccessFile(new File(tfsPath), "rw");
        tfChannel = tfFile.getChannel();
        RandomAccessFile skipFile = new RandomAccessFile(new File(skipsPath), "rw");
        skipChannel = skipFile.getChannel();
        DocumentIndex d = new DocumentIndex();
        docIndex = d.getDocIndex();
    }

    /**
     * query processing algorithm to process a query in conjunctive mode: for each document checks if it's present in all the lists
     * in that case it computes for each term in the query the score of the document
     * @param query: query given in input to be processed
     * @param k: number of top documents to retrieve
     * @param mode: 0 if the scoring function is TFIDF, 1 if it's BM25
     * @throws IOException
     * @return the list of top k documents ordered by decreasing score
     */
    public List<ScoreEntry> conjunctiveDaat(String query, int k, int mode) throws IOException {
        List<String> proQuery = preprocessing.preprocessDocument(query);
        //duplicate filtering
        List<String> terms = new ArrayList<>(new HashSet<>(proQuery));
        int queryLen = terms.size();
        lexicon = new HashMap<>();
        TreeSet<ScoreEntry> scores = new TreeSet<>(); //to store partial scores results
        for(String term: terms){
            LexiconStats l;
            //if the term is in cache we retrieve the pointer from it, otherwise we do binary search on the lexicon file
            if(cacheTerms.get(term)!=null){
                l = new LexiconStats(cacheTerms.get(term));
            }
            else{
                l = Utils.getPointer(lexChannel, term);
                LexiconStats cachedLex = new LexiconStats(l);
                cacheTerms.put(term,cachedLex);
            }
            //check if the term was present in the lexicon or not
            if(l.getdF()!=0){
                lexicon.put(term, l);
            }
            else{
                queryLen--;
            }
        }
        if(queryLen==0) return null; //if no query term was in the lexicon
        //initialize data structures
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numBlocks = new int[queryLen];
        endDocids = new int[queryLen];
        docIdsIt = new Iterator[queryLen];
        tfsIt = new Iterator[queryLen];
        String[] queryTerms = new String[queryLen];
        for(int i = 0; i < queryLen; i++){
            String term = terms.get(i);
            lexicon.get(term).setIndex(i);
            lexicon.get(term).setCurdoc(0);
            queryTerms[i] = term;
            openList(docChannel, tfChannel, skipChannel, queryTerms[i]);
        }
        int did = getMinDocid(queryTerms);
        while (did < maxDocID){
            double score = 0.0;
            did = nextGEQ(terms.get(0), did);
            if(did == maxDocID){
                break;
            }
            int d = 0;
            for (int i=1; (i<queryLen) && ((d=nextGEQ(queryTerms[i], did)) == did); i++);
            if (d > did){
                did = d; // not in intersection
            }
            else {
                //docID is in intersection; now get all frequencies
                for (int i=0; i<queryTerms.length; i++){
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    if(mode == 1) {
                        //compute BM25 score from frequencies and document length
                        int docLen = docIndex.get(did);
                        score += Scorer.bm25Weight(tf, docLen, idf);
                    }
                    else{
                        //compute TFIDF score from frequencies
                        score += Scorer.tfidf(tf, idf);
                    }
                }
                scores.add(new ScoreEntry(did, score));
                if(scores.size() > k){
                    scores.pollFirst(); //remove the minimum element
                }
                did++;
            }
        }
        // Return the result list
        return scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList());
    }

    /**
     * query processing algorithm to process a query in disjunctive mode: it's implemented with a MaxScore dynamic pruning algorithm
     * for each term the document upper bounds are computed and the posting lists are divided in essential and non-essential through
     * a pivot variable, which marks the beginning of essential lists (terms are ordered by increasing term upper bound)
     * the list is essential if the document upper bound is above the threshold, otherwise is non-essential
     * the threshold is the smallest value in the priority queue of top k scores, if the number of scores is less than k is equal to 0
     * @param query: query given in input to be processed
     * @param k: number of top documents to retrieve
     * @param mode: 0 if the scoring function is TFIDF, 1 if it's BM25
     * @throws IOException
     * @return the list of top k documents ordered by decreasing score
     */
    public List<ScoreEntry> disjunctiveDaat(String query, int k, int mode) throws IOException {
        if(k==0) return null;
        List<String> proQuery = preprocessing.preprocessDocument(query);
        //duplicate filtering
        List<String> terms = new ArrayList<>(new HashSet<>(proQuery));
        int queryLen = terms.size();
        lexicon = new HashMap<>();
        TreeSet<ScoreEntry> scores = new TreeSet<>(); //to store partial scores results
        for(String term: terms){
            LexiconStats l;
            //if the term is in cache we retrieve the pointers from it; otherwise we do binary search on the lexicon file
            if(cacheTerms.get(term)!=null){
                l = new LexiconStats(cacheTerms.get(term));
            }
            else{
                l = Utils.getPointer(lexChannel, term);
                if(l.getdF()!=0) {
                    LexiconStats cachedLex = new LexiconStats(l);
                    cacheTerms.put(term, cachedLex);
                }
            }
            //check if the term was present in the lexicon or not
            if(l.getdF()!=0){
                lexicon.put(term, l);
            }
            else{
                queryLen--;
            }
        }
        if(queryLen==0) return null; //if no query term was present in the lexicon
        //initialize data structures
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numBlocks = new int[queryLen];
        endDocids = new int[queryLen];
        docIdsIt = new Iterator[queryLen];
        tfsIt = new Iterator[queryLen];
        //take the term upper bounds, ordered in increasing order
        List<TermUB> termUBs = new ArrayList<>();
        for(String term: lexicon.keySet()){
            if(mode == 1) {
                termUBs.add(new TermUB(term, lexicon.get(term).getTermUpperBound()));
            }
            else{
                termUBs.add(new TermUB(term, lexicon.get(term).getTermUpperBoundTfIdf()));
            }
        }
        Collections.sort(termUBs);
        String [] queryTerms = new String[queryLen];
        int pivot = 0;
        double[] documentUB = new double[queryLen];
        double prec = 0.0;
        int index = 0;
        //order query terms by increasing term upper bound
        for (TermUB tub: termUBs){
            documentUB[index] = tub.getMaxScore() + prec;
            prec = documentUB[index];
            queryTerms[index] = tub.getTerm();
            index++;
        }
        for(int i = 0; i < queryLen; i++){
            lexicon.get(queryTerms[i]).setIndex(i);
            lexicon.get(queryTerms[i]).setCurdoc(0);
            openList(docChannel, tfChannel, skipChannel, queryTerms[i]);
        }
        double threshold = 0.0;
        int next;
        int did = getMinDocid(queryTerms); //take the first docID
        while (pivot < queryLen && did != maxDocID){
            next = maxDocID;
            double score = 0.0;
            //process essential lists
            for (int i=pivot; i<queryLen; i++){
                int current = nextGEQ(queryTerms[i], did);
                if(current == did){
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    if(mode == 1) {
                        //compute BM25 score from frequencies and document length
                        int docLen = docIndex.get(did);
                        score += Scorer.bm25Weight(tf, docLen, idf);
                    }
                    else{
                        //compute TFIDF score from frequencies
                        score += Scorer.tfidf(tf, idf);
                    }
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
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    if(mode == 1) {
                        //compute BM25 score from frequencies and document length
                        int docLen = docIndex.get(did);
                        score += Scorer.bm25Weight(tf, docLen, idf);
                    }
                    else{
                        //compute TFIDF score from frequencies
                        score += Scorer.tfidf(tf, idf);
                    }
                }
            }
            //check if the new threshold is higher than previous one, in this case update the threshold
            scores.add(new ScoreEntry(did, score));
            if(scores.size() > k){
                scores.pollFirst(); //remove the minimum element
            }
            double min = scores.first().getScore();
            if(scores.size() == k && min > threshold) {
                threshold = min;
                //update pivot
                while (pivot < queryLen && documentUB[pivot] <= threshold) {
                    pivot++;
                }
            }
            did = next;
        }
        return scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList());
    }

    /**
     * query processing algorithm to process a query in disjunctive mode, used to test trec_eval
     * @param query: query given in inptu to be processed
     * @param k: number of top documents to retrieve
     * @param mode: false if the scoring function is TFIDF, true if it's BM25
     * @throws IOException
     * @return the top document of the list of top k documents
     */
    public ScoreEntry disjunctiveDaatEval(String query, int k, boolean mode) throws IOException {
        List<String> proQuery = preprocessing.preprocessDocument(query);
        //duplicate filtering
        List<String> terms = new ArrayList<>(new HashSet<>(proQuery));
        int queryLen = terms.size();
        TreeSet<ScoreEntry> scores = new TreeSet<>(); //to store partial scores results
        lexicon = new HashMap<>();
        for (String term : terms) {
            LexiconStats l;
            if (cacheTerms.get(term) != null) {
                l = new LexiconStats(cacheTerms.get(term));
            } else {
                l = Utils.getPointer(lexChannel, term);
                if (l.getdF() != 0) {
                    LexiconStats cachedLex = new LexiconStats(l);
                    cacheTerms.put(term, cachedLex);
                }
            }
            if (l.getdF() != 0) {
                lexicon.put(term, l);
            } else {
                queryLen--;
            }
        }
        if (queryLen == 0) {
            return new ScoreEntry(maxDocID, 0);
        }
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numBlocks = new int[queryLen];
        endDocids = new int[queryLen];
        docIdsIt = new Iterator[queryLen];
        tfsIt = new Iterator[queryLen];
        List<TermUB> termUBs = new ArrayList<>();
        for (String term : lexicon.keySet()) {
            if (mode) {
                termUBs.add(new TermUB(term, lexicon.get(term).getTermUpperBound()));
            } else {
                termUBs.add(new TermUB(term, lexicon.get(term).getTermUpperBoundTfIdf()));
            }
        }
        Collections.sort(termUBs);
        String[] queryTerms = new String[queryLen];
        int pivot = 0;
        double[] documentUB = new double[queryLen];
        double prec = 0.0;
        int index = 0;
        for (TermUB tub : termUBs) {
            documentUB[index] = tub.getMaxScore() + prec;
            prec = documentUB[index];
            queryTerms[index] = tub.getTerm();
            index++;
        }
        for (int i = 0; i < queryLen; i++) {
            lexicon.get(queryTerms[i]).setIndex(i);
            lexicon.get(queryTerms[i]).setCurdoc(0);
            openList(docChannel, tfChannel, skipChannel, queryTerms[i]);
        }
        double threshold = 0.0;
        int next;
        int did = getMinDocid(queryTerms);
        while (pivot < queryLen && did != maxDocID) {
            next = maxDocID;
            double score = 0.0;
            //process essential lists
            for (int i = pivot; i < queryLen; i++) {
                int current = nextGEQ(queryTerms[i], did);
                if (current == did) {
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    if (mode) {
                        //compute BM25 score from frequencies and document length
                        int docLen = docIndex.get(did);
                        score += Scorer.bm25Weight(tf, docLen, idf);
                    } else {
                        //compute TFIDF score from frequencies
                        score += Scorer.tfidf(tf, idf);
                    }
                    current = nextGEQ(queryTerms[i], did + 1); //update the pointer to next docid
                }
                if ((current < next)) {
                    next = current;
                }
            }
            //process non essential lists
            for (int i = pivot - 1; i >= 0; i--) {
                //check document upper bound
                if (documentUB[i] + score <= threshold) {
                    break;
                }
                int current = nextGEQ(queryTerms[i], did);
                if (current == did) {
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    if (mode) {
                        //compute BM25 score from frequencies and document length
                        int docLen = docIndex.get(did);
                        score += Scorer.bm25Weight(tf, docLen, idf);
                    } else {
                        //compute TFIDF score from frequencies
                        score += Scorer.tfidf(tf, idf);
                    }
                }
            }
            //check if the new threshold is higher than previous one, in this case update the threshold
            scores.add(new ScoreEntry(did, score));
            if (scores.size() > k) {
                scores.pollFirst(); //remove the minimum element
            }
            double min = scores.first().getScore();
            if (scores.size() == k && min > threshold) {
                threshold = min;
                //update pivot
                while (pivot < queryLen && documentUB[pivot] <= threshold) {
                    pivot++;
                }
            }
            did = next;
        }
        if (scores.size() > 0) {
            ScoreEntry result = scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList()).get(0);
            return result;
        }
        return new ScoreEntry(maxDocID, 0.0);
    }

    /**
     * function that returns the first docID to process among the docIDs of the posting lists of the terms of the query
     * @param queryTerms: array containing the query terms
     * @throws IOException
     * @return the first docID to process
     */
    private int getMinDocid(String[] queryTerms) throws IOException {
        int did = nextGEQ(queryTerms[0], 1); //start from 1, the first possible docId
        for (int i=1; (i<queryTerms.length); i++){
            int d= nextGEQ(queryTerms[i], did);
            if(d<did){ //take the lowest docId as a starting point
                did = d;
            }
            //reset iterators
            docIdsIt[lexicon.get(queryTerms[i]).getIndex()] = decompressedDocIds[lexicon.get(queryTerms[i]).getIndex()].iterator();
            tfsIt[lexicon.get(queryTerms[i]).getIndex()] = decompressedTfs[lexicon.get(queryTerms[i]).getIndex()].iterator();
        }
        //reset iterators for the first list
        docIdsIt[lexicon.get(queryTerms[0]).getIndex()] = decompressedDocIds[lexicon.get(queryTerms[0]).getIndex()].iterator();
        tfsIt[lexicon.get(queryTerms[0]).getIndex()] = decompressedTfs[lexicon.get(queryTerms[0]).getIndex()].iterator();
        return did;
    }

    /**
     * method that takes in input the current docID we are processing and returns the first docID greater or equal to the input value
     * if the value is not present in the block, we open the next one, if there is a next block to process
     * @param term: term of the query for which we are looking for the next docID to process
     * @param value: current docID we are processing
     * @throws IOException
     * @return the next docID to process
     */
    private int nextGEQ(String term, int value) throws IOException {
        Iterator<Integer> itDocs = docIdsIt[lexicon.get(term).getIndex()];
        Iterator<Integer> itTfs = tfsIt[lexicon.get(term).getIndex()];
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
            docIdsIt[lexicon.get(term).getIndex()] = itDocs; //update the iterator
            tfsIt[lexicon.get(term).getIndex()] = itTfs;
            //check if we are in a new block
            if(value >= endDocids[lexicon.get(term).getIndex()] || docId == prec){
                if(numBlocks[lexicon.get(term).getIndex()]+ ConfigurationParameters.SKIP_BLOCK_SIZE >lexicon.get(term).getSkipLen()){
                    return maxDocID;
                }
                openList(docChannel, tfChannel, skipChannel, term);
                itDocs = docIdsIt[lexicon.get(term).getIndex()];
                itTfs = tfsIt[lexicon.get(term).getIndex()];
            }
        }
        // If no such value was found, return a special value indicating that the search failed
        return maxDocID;
    }

    /**
     * method that open the decompressed block that we currently need to process, taking it from the docIDs and term frequencies file
     * using the skip information and the data structures that need to be updated after opening the new block
     * @param docChannel: file channel of the docIDs file
     * @param tfChannel: file channel of the term frequencies file
     * @param skips: file channel of the skip information file
     * @param term:
     * @throws IOException
     */
    public void openList(FileChannel docChannel, FileChannel tfChannel, FileChannel skips, String term) throws IOException {
        // Read the posting list block data from the file
        Compressor compressor = new Compressor();
        skips.position(lexicon.get(term).getOffsetSkip() + numBlocks[lexicon.get(term).getIndex()]);
        ByteBuffer skipInfo = ByteBuffer.allocate(lexicon.get(term).getSkipLen() - numBlocks[lexicon.get(term).getIndex()]);
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
        decompressedDocIds[lexicon.get(term).getIndex()] = compressor.variableByteDecode(docIds.array());
        decompressedTfs[lexicon.get(term).getIndex()] = compressor.unaryDecode(tfs.array());
        //update the skip blocks read so far
        numBlocks[lexicon.get(term).getIndex()] += ConfigurationParameters.SKIP_BLOCK_SIZE;
        //instantiate the iterators
        docIdsIt[lexicon.get(term).getIndex()] = decompressedDocIds[lexicon.get(term).getIndex()].iterator();
        tfsIt[lexicon.get(term).getIndex()] = decompressedTfs[lexicon.get(term).getIndex()].iterator();
        //update offsets
        lexicon.get(term).setOffsetDocid(lexicon.get(term).getOffsetDocid()+skipdocid);
        lexicon.get(term).setOffsetTf(lexicon.get(term).getOffsetTf()+skiptf);
    }
}
