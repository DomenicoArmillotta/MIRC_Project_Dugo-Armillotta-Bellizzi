package queryProcessing;

import compression.Compressor;
import fileManager.ConfigurationParameters;
import indexing.DocumentIndex;
import invertedIndex.LexiconStats;
import org.junit.platform.commons.util.LruCache;
import preprocessing.Preprocessor;
import utility.Cache;
import utility.CacheTerms;
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
    private LruCache<String,ScoreEntry> cache = new LruCache<>(5000); //to cache the query results
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

    public List<ScoreEntry> conjunctiveDaat(String query, int k, int mode) throws IOException {
        List<String> proQuery = preprocessing.preprocessDocument(query);
        //duplicate filtering
        List<String> terms = new ArrayList<>(new HashSet<>(proQuery));
        int queryLen = terms.size();
        lexicon = new HashMap<>();
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numBlocks = new int[queryLen];
        endDocids = new int[queryLen];
        docIdsIt = new Iterator[queryLen];
        tfsIt = new Iterator[queryLen];
        TreeSet<ScoreEntry> scores = new TreeSet<>(); //to store partial scores results
        for(String term: terms){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            if(l.getdF()!=0){
                lexicon.put(term, l);
            }
            else{
                queryLen--;
            }
        }
        if(queryLen==0) return null;
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
        //Put the results in cache
        if(scores.size()>0)
            cache.put(query, scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList()).get(0));
        // Return the result list
        return scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList());
    }

    public List<ScoreEntry> disjunctiveDaat(String query, int k, int mode) throws IOException {
        if(k==0) return null;
        List<String> proQuery = preprocessing.preprocessDocument(query);
        //duplicate filtering
        List<String> terms = new ArrayList<>(new HashSet<>(proQuery));
        int queryLen = terms.size();
        lexicon = new HashMap<>();
        TreeSet<ScoreEntry> scores = new TreeSet<>(); //to store partial scores results
        for(String term: terms){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            if(l.getdF()!=0){
                lexicon.put(term, l);
            }
            else{
                queryLen--;
            }
        }
        if(queryLen==0) return null;
        System.out.println(lexicon.keySet());
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numBlocks = new int[queryLen];
        endDocids = new int[queryLen];
        docIdsIt = new Iterator[queryLen];
        tfsIt = new Iterator[queryLen];
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
        int did = getMinDocid(queryTerms);
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
        //Put the results in cache
        if(scores.size()>0)
            cache.put(query, scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList()).get(0));
        return scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList());
    }

    public ScoreEntry disjunctiveDaatEval(String query, int k, boolean mode) throws IOException {
        if(cache.get(query)!=null){
            return cache.get(query);
        }
        List<String> proQuery = preprocessing.preprocessDocument(query);
        //duplicate filtering
        List<String> terms = new ArrayList<>(new HashSet<>(proQuery));
        int queryLen = terms.size();
        TreeSet<ScoreEntry> scores = new TreeSet<>(); //to store partial scores results
        lexicon = new HashMap<>();
        for(String term: terms){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            /*if(cacheTerms.get(term)!=null){
                l = cacheTerms.get(term);
            }
            else{
                l = Utils.getPointer(lexChannel, term);
                cacheTerms.put(term,l);
            }*/
            if(l.getdF()!=0){
                lexicon.put(term, l);
            }
            else{
                queryLen--;
            }
        }
        if(queryLen==0){
            return new ScoreEntry(maxDocID, 0);
        }
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numBlocks = new int[queryLen];
        endDocids = new int[queryLen];
        docIdsIt = new Iterator[queryLen];
        tfsIt = new Iterator[queryLen];
        List<TermUB> termUBs = new ArrayList<>();
        for(String term: lexicon.keySet()){
            if(mode) {
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
        int did = getMinDocid(queryTerms);
        while (pivot < queryLen && did != maxDocID){
            next = maxDocID;
            double score = 0.0;
            //process essential lists
            for (int i=pivot; i<queryLen; i++){
                int current = nextGEQ(queryTerms[i], did);
                if(current == did){
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    if(mode) {
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
                    if(mode) {
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
        //Put the results in cache
        if(scores.size()>0) {
            ScoreEntry result = scores.stream().sorted(Collections.reverseOrder()).collect(Collectors.toList()).get(0);
            cache.put(query, result);
            return result;
        }
        return new ScoreEntry(maxDocID,0.0);
    }

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

    //TODO: fare trec eval!!!!!

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
            docIdsIt[lexicon.get(term).getIndex()] = itDocs; //update the iterator (non so se serve)
            tfsIt[lexicon.get(term).getIndex()] = itTfs;
            //check if we are in a new block
            if(value >= endDocids[lexicon.get(term).getIndex()] || docId == prec){
                if(numBlocks[lexicon.get(term).getIndex()]+ ConfigurationParameters.SKIP_BLOCK_SIZE >lexicon.get(term).getSkipLen()){
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
