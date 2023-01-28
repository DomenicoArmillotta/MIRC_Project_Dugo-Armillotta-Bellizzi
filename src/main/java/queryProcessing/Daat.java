package queryProcessing;

import compression.Compressor;
import fileManager.ConfigurationParameters;
import invertedIndex.CompressedList;
import invertedIndex.InvertedIndex;
import invertedIndex.LexiconStats;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.C;
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
    private int[] numPosting;
    private int[] endDocids;
    private Iterator<Integer>[] docIdsIt;
    private Iterator<Integer>[] tfsIt;
    private String lexiconPath = "docs/lexiconTot.txt";
    private String docidsPath = "docs/docids.txt";
    private String tfsPath = "docs/tfs.txt";
    private String docIndexPath = "docs/docIndex.txt";
    private String skipsPath = "docs/skipInfo.txt";

    private CompressedList[] postingLists; //to keep the compressed posting list for each term of the query

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
    }


    public List<Map.Entry<Integer,Double>> conjunctiveDaat(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = preprocessing.preprocess_doc(query);
        int queryLen = proQuery.size();
        postingLists = new CompressedList[queryLen];
        decompressedDocIds = new List[queryLen];
        decompressedTfs = new List[queryLen];
        numPosting = new int[queryLen];
        endDocids = new int[queryLen];
        TreeMap<Integer, Double> scores = new TreeMap<>();
        for(String term: proQuery){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        for(int i = 0; i < queryLen; i++){
            postingLists[i] = openList(docChannel, tfChannel, skipChannel, proQuery.get(i));
            lexicon.get(proQuery.get(i)).setIndex(i);
            numPosting[i] = lexicon.get(proQuery.get(i)).getdF();
        }
        //TODO: sort query terms in lexicon by shortest list
        int did = 0;
        while (did <= maxDocID){
            double score = 0.0;
            did = nextGEQ(proQuery.get(0), did);
            if(did == -1){
                break;
            }
            int d = 0;
            for (int i=1; (i<queryLen) && ((d=nextGEQ(proQuery.get(i), did)) == did); i++);
            if (d > did){
                did = d; // not in intersection
            }
            else
            {
                //docID is in intersection; now get all frequencies
                for (int i=0; i<proQuery.size(); i++){
                    int tf = lexicon.get(proQuery.get(i)).getCurTf();
                    //System.out.println("tf: " + proQuery.get(i) + " " + did + " " + tf[i]);
                    int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    double idf = lexicon.get(proQuery.get(i)).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.bm25Weight(tf, docLen, idf); //to modify after getFreq
                }
                Map.Entry<Integer, Double> minEntry = null;
                if(scores.firstEntry() == null){
                    scores.put(did, score);
                }
                else{
                    for (Map.Entry<Integer, Double> entry : scores.entrySet())
                    {
                        if (minEntry == null || entry.getValue().compareTo(minEntry.getValue()) < 0)
                        {
                            minEntry = entry;
                        }
                    }
                    if(minEntry.getValue() <= score){
                        scores.put(did, score);
                    }
                }
                if(scores.size() > k){
                    scores.remove(minEntry.getKey(), minEntry.getValue());
                }
                did++; //and increase did to search for next post
            }
        }
        // Return the result list
        return scores.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());
    }

    public List<Map.Entry<Integer,Double>> conjunctiveDaatTfIdf(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = preprocessing.preprocess_doc(query);
        int queryLen = proQuery.size();
        postingLists = new CompressedList[queryLen];
        decompressedDocIds = new List[queryLen];
        decompressedTfs = new List[queryLen];
        numPosting = new int[queryLen];
        endDocids = new int[queryLen];
        TreeMap<Integer, Double> scores = new TreeMap<>();
        for(String term: proQuery){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        postingLists = new CompressedList[queryLen];
        for(int i = 0; i < queryLen; i++){
            postingLists[i] = openList(docChannel, tfChannel, skipChannel, proQuery.get(i));
            lexicon.get(proQuery.get(i)).setIndex(i);
            numPosting[i] = lexicon.get(proQuery.get(i)).getdF();
        }
        //TODO: sort query terms in lexicon by shortest list
        int did = 0;
        while (did <= maxDocID){
            double score = 0.0;
            did = nextGEQ(proQuery.get(0), did);
            if(did == -1){
                break;
            }
            int d = 0;
            for (int i=1; (i<queryLen) && ((d=nextGEQ(proQuery.get(i), did)) == did); i++);
            if (d > did){
                did = d; // not in intersection
            }
            else
            {
                //docID is in intersection; now get all frequencies
                for (int i=0; i<proQuery.size(); i++){
                    int tf = lexicon.get(proQuery.get(i)).getCurTf();
                    //System.out.println("tf: " + proQuery.get(i) + " " + did + " " + tf[i]);
                    int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    double idf = lexicon.get(proQuery.get(i)).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.tfidf(tf, docLen, idf); //to modify after getFreq
                }
                Map.Entry<Integer, Double> minEntry = null;
                if(scores.firstEntry() == null){
                    scores.put(did, score);
                }
                else{
                    for (Map.Entry<Integer, Double> entry : scores.entrySet())
                    {
                        if (minEntry == null || entry.getValue().compareTo(minEntry.getValue()) < 0)
                        {
                            minEntry = entry;
                        }
                    }
                    if(minEntry.getValue() <= score){
                        scores.put(did, score);
                    }
                }
                if(scores.size() > k){
                    scores.remove(minEntry.getKey(), minEntry.getValue());
                }
                did++; //and increase did to search for next post
            }
        }
        // Return the result list
        return scores.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());
    }

    public List<Map.Entry<Integer, Double>> disjunctiveDaat(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = preprocessing.preprocess_doc(query);
        int queryLen = proQuery.size();
        postingLists = new CompressedList[queryLen];
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numPosting = new int[queryLen];
        endDocids = new int[queryLen];
        docIdsIt = new Iterator[queryLen];
        tfsIt = new Iterator[queryLen];
        TreeMap<Integer, Double> scores = new TreeMap<>(); //to store partial scores results
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
            //maxScores.put(term, lexicon.get(term).getTermUpperBound());
        }
        for(int i = 0; i < queryLen; i++){
            lexicon.get(queryTerms[i]).setIndex(i);
            lexicon.get(queryTerms[i]).setCurdoc(0);
            openListNew(docChannel, tfChannel, skipChannel, queryTerms[i]);
            //postingLists[i] = openList(docChannel, tfChannel, skipChannel, queryTerms[i]);
            //numPosting[i] = lexicon.get(queryTerms[i]).getdF();
        }
        HashMap<Integer, Integer> docLens = new HashMap<>();
        int pivot = 0;
        double[] documentUB = new double[queryLen];
        double prec = 0.0;
        int index = 0;
        for(double maxScore: termUB){
            documentUB[index] = maxScore + prec;
            prec = documentUB[index];
            System.out.println(queryTerms[index] + " " + documentUB[index]);
            index++;
        }
        double threshold = 0;
        int next;
        int did = getMinDocid(queryTerms);
        System.out.println(did);

        while (pivot < queryLen && did != maxDocID){
            next = maxDocID;
            double score = 0.0;
            //process essential lists
            for (int i=pivot; i<queryLen; i++){
                int current = nextGEQNew(queryTerms[i], did);
                //System.out.println(queryTerms[i] + ", Current: " + current +", Did: " + did);
                if(current == did){
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    //int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    int docLen = 20;
                    /*if(docLens.get(did) == null){
                        docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                        docLens.put(did, docLen);
                    }
                    else{
                        docLen = docLens.get(did);
                    }*/
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.bm25Weight(tf, docLen, idf);
                    current = nextGEQNew(queryTerms[i], did+1); //update the pointer to next docid
                    //System.out.println("Updated to " + current);
                    //System.out.println("Partial score for " + queryTerms[i] + " " + did + ": " + score);
                }
                if((current < next)){
                    //System.out.println("Updated from " + next + " to " + current);
                    next = current;
                }
            }
            //process non essential lists
            for (int i=pivot-1; i>=0; i--){
                //System.out.println("Non essential: " + queryTerms[i]);
                //check document upper bound
                if(documentUB[i] + score <= threshold){
                    break;
                }
                int current = nextGEQNew(queryTerms[i], did);
                if(current == did) {
                    //System.out.println("Non essential: " + queryTerms[i] + " processed at " + did);
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    //int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    int docLen =20;
                    /*if(docLens.get(did) == null){
                        docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                        docLens.put(did, docLen);
                    }
                    else{
                        docLen = docLens.get(did);
                    }*/
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.bm25Weight(tf, docLen, idf);
                    //System.out.println("Partial score for " + queryTerms[i] + " " + did + ": " + score);
                }
            }
            //System.out.println("Partial score for " + " " + did + ": " + score);
            //update pivot
            //check if the new threshold is higher than previous one, in this case update the threshold
            Map.Entry<Integer, Double> minEntry = null;
            if(scores.firstEntry() == null){
                scores.put(did, score);
            }
            else{
                for (Map.Entry<Integer, Double> entry : scores.entrySet())
                {
                    if (minEntry == null || entry.getValue().compareTo(minEntry.getValue()) < 0)
                    {
                        minEntry = entry;
                    }
                }
                if(minEntry.getValue() < score){
                    scores.put(did, score);
                    if(scores.size() > k) {
                        scores.remove(minEntry.getKey(), minEntry.getValue());
                        //System.out.println("Scores at: " + did + ": " + scores);
                    }
                }
            }
            //System.out.println("Score for " + did + ": " + score);
            if(scores.size() == k && minEntry.getValue() > threshold){
                threshold = minEntry.getValue();
                while(pivot < queryLen && documentUB[pivot]<= threshold){
                    pivot++;
                }
                //System.out.println("updated threshold: " + threshold + " " + pivot);
            }
            did = next;
            //System.out.println("Next did: " + did);
        }
        //Use Comparator.reverseOrder() for reverse ordering
        return scores.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());
    }

    public List<Map.Entry<Integer, Double>> disjunctiveDaatTfIdf(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = preprocessing.preprocess_doc(query);
        int queryLen = proQuery.size();
        postingLists = new CompressedList[queryLen];
        decompressedDocIds = new ArrayList[queryLen];
        decompressedTfs = new ArrayList[queryLen];
        numPosting = new int[queryLen];
        endDocids = new int[queryLen];
        TreeMap<Integer, Double> scores = new TreeMap<>(); //to store partial scores results
        double[] termUB = new double[queryLen];
        for(String term: proQuery){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        for(int i = 0; i < queryLen; i++){
            termUB[i] = lexicon.get(proQuery.get(i)).getTermUpperBoundTf();
        }
        Arrays.sort(termUB);
        String [] queryTerms = new String[queryLen];
        for(String term: proQuery){
            double ub = lexicon.get(term).getTermUpperBoundTf();
            int i = Arrays.binarySearch(termUB, ub);
            queryTerms[i] = term;
            //maxScores.put(term, lexicon.get(term).getTermUpperBound());
        }
        for(int i = 0; i < queryLen; i++){
            postingLists[i] = openList(docChannel, tfChannel, skipChannel, queryTerms[i]);
            lexicon.get(queryTerms[i]).setIndex(i);
            numPosting[i] = lexicon.get(queryTerms[i]).getdF();
        }
        HashMap<Integer, Integer> docLens = new HashMap<>();
        int pivot = 0;
        double[] documentUB = new double[queryLen];
        double prec = 0.0;
        int index = 0;
        for(double maxScore: termUB){
            documentUB[index] = maxScore + prec;
            prec = documentUB[index];
            System.out.println(queryTerms[index] + " " + documentUB[index]);
            index++;
        }
        double threshold = 0;
        int next;
        int did = 0;
        did = nextGEQ(queryTerms[0], did);
        for (int i=1; (i<queryLen); i++){
            int d=nextGEQ(queryTerms[i], did);
            if(d<did){
                did = d;
            }
        }
        while (pivot < queryLen && did != -1){
            next = maxDocID;
            double score = 0.0;
            //process essential lists
            for (int i=pivot; i<queryLen; i++){
                int current = nextGEQ(queryTerms[i], did);
                if(current == did){
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    //int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    int docLen;
                    if(docLens.get(did) == null){
                        docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                        docLens.put(did, docLen);
                    }
                    else{
                        docLen = docLens.get(did);
                    }
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.tfidf(tf, docLen, idf);
                    current = nextGEQ(queryTerms[i], did+1); //update the pointer to next docid
                }
                if(current < next){
                    next = current;
                }
            }
            //process non essential lists
            for (int i=pivot-1; i>=0; i--){
                //System.out.println("Non essential: " + queryTerms[i]);
                //check document upper bound
                if(documentUB[i] + score <= threshold){
                    break;
                }
                int current = nextGEQ(queryTerms[i], did);
                if(current == did) {
                    //System.out.println("Non essential: " + queryTerms[i] + " processed at " + did);
                    int tf = lexicon.get(queryTerms[i]).getCurTf();
                    //int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    int docLen;
                    if(docLens.get(did) == null){
                        docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                        docLens.put(did, docLen);
                    }
                    else{
                        docLen = docLens.get(did);
                    }
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.tfidf(tf, docLen, idf);
                    //System.out.println("Partial score for " + queryTerms[i] + " " + did + ": " + score);
                }
            }
            //update pivot
            //check if the new threshold is higher than previous one, in this case update the threshold
            Map.Entry<Integer, Double> minEntry = null;
            if(scores.firstEntry() == null){
                scores.put(did, score);
            }
            else{
                for (Map.Entry<Integer, Double> entry : scores.entrySet())
                {
                    if (minEntry == null || entry.getValue().compareTo(minEntry.getValue()) < 0)
                    {
                        minEntry = entry;
                    }
                }
                if(minEntry.getValue() < score){
                    scores.put(did, score);
                    if(scores.size() > k) {
                        scores.remove(minEntry.getKey(), minEntry.getValue());
                        //System.out.println("Scores at: " + did + ": " + scores);
                    }
                }
            }
            //System.out.println("Score for " + did + ": " + score);
            if(scores.size() == k && minEntry.getValue() > threshold){
                threshold = minEntry.getValue();
                //System.out.println("updated threshold: " + threshold);
                while(pivot < queryLen && documentUB[pivot]<= threshold){
                    pivot++;
                }
            }
            did = next;
            //System.out.println("Next did: " + did);
        }
        //Use Comparator.reverseOrder() for reverse ordering
        return scores.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toList());

    }

    //TODO: da modificare dopo aver modificato la gestione delle liste
    private int nextGEQ(String term, int value) throws IOException {
        Compressor c = new Compressor();
        //take number of posting in the list and in the blocks
        int nPostings = numPosting[lexicon.get(term).getIndex()];
        int n = (int) Math.floor(Math.sqrt(lexicon.get(term).getdF()));
        int i = 0;
        ByteBuffer docIds = ByteBuffer.wrap(postingLists[lexicon.get(term).getIndex()].getDocids());
        ByteBuffer skipInfo = ByteBuffer.wrap(postingLists[lexicon.get(term).getIndex()].getSkipInfo());
        ByteBuffer tfs = ByteBuffer.wrap(postingLists[lexicon.get(term).getIndex()].getTfs());
        skipInfo.position(0);
        docIds.position(0);
        tfs.position(0);
        while(i< nPostings) { //we need to update the index; check if we are in the last block
            int endocid = skipInfo.getInt();
            int skipdocid = skipInfo.getInt();
            int skiptf = skipInfo.getInt();
            ByteBuffer blockDocId = ByteBuffer.allocate(skipdocid);
            docIds.get(blockDocId.array(), 0, skipdocid);
            ByteBuffer blockTf = ByteBuffer.allocate(skipdocid);
            tfs.get(blockTf.array(), 0, skiptf);
            if(endocid >= value) {
                //we do this to decompress only one time when we first enter the block, and not each time we enter
                if(decompressedDocIds[lexicon.get(term).getIndex()] == null){
                    decompressedDocIds[lexicon.get(term).getIndex()] = c.variableByteDecodeBlock(blockDocId.array(), n);
                    decompressedTfs[lexicon.get(term).getIndex()] = c.unaryDecodeBlock(blockTf.array(), n);
                }
                int index = 0;
                for (int docId : decompressedDocIds[lexicon.get(term).getIndex()]) {
                    if (docId >= value) {
                        lexicon.get(term).setCurTf(decompressedTfs[lexicon.get(term).getIndex()].get(index));
                        int docIdSize = decompressedDocIds[lexicon.get(term).getIndex()].size();
                        int tfSize = decompressedTfs[lexicon.get(term).getIndex()].size();
                        if(docIdSize>1) {
                            List<Integer> tmpDocIds = decompressedDocIds[lexicon.get(term).getIndex()];
                            List<Integer> tmpTfs = decompressedTfs[lexicon.get(term).getIndex()];
                            tmpDocIds.remove(0);
                            tmpTfs.remove(0);
                            decompressedDocIds[lexicon.get(term).getIndex()] = null;
                            decompressedDocIds[lexicon.get(term).getIndex()] = new ArrayList<>(tmpDocIds);
                            decompressedTfs[lexicon.get(term).getIndex()] = null;
                            decompressedTfs[lexicon.get(term).getIndex()] = new ArrayList<>(tmpTfs);
                            /*decompressedDocIds[lexicon.get(term).getIndex()].remove(0);
                            decompressedTfs[lexicon.get(term).getIndex()].remove(0);*/
                        }
                        return docId;
                    }
                    index++;
                }
            }
            //update posting lists removing the old block
            //skipInfo.position(0);
            //docIds.position(0);
            //tfs.position(0);
            int newSkipLen = lexicon.get(term).getSkipLen()-12;
            int newDocIdsLen = lexicon.get(term).getDocidsLen()-skipdocid;
            int newTfLen = lexicon.get(term).getTfLen()-skiptf;
            ByteBuffer newBlock = ByteBuffer.allocate(newSkipLen);
            ByteBuffer newBlockDocId = ByteBuffer.allocate(newDocIdsLen);
            ByteBuffer newBlockTf = ByteBuffer.allocate(newTfLen);
            skipInfo.get(newBlock.array(), 0, newSkipLen);
            docIds.get(newBlockDocId.array(), 0, newDocIdsLen);
            tfs.get(newBlockTf.array(), 0, newTfLen);
            docIds = newBlockDocId;
            skipInfo = newBlock;
            tfs = newBlockTf;
            lexicon.get(term).setSkipLen(newSkipLen);
            lexicon.get(term).setDocidsLen(newDocIdsLen);
            lexicon.get(term).setTfLen(newTfLen);
            numPosting[lexicon.get(term).getIndex()] -= n;
            postingLists[lexicon.get(term).getIndex()] = new CompressedList(newBlockDocId.array(), newBlockTf.array(), newBlock.array());
            decompressedDocIds[lexicon.get(term).getIndex()] = null;
            decompressedTfs[lexicon.get(term).getIndex()] = null;
            if(i+1 == nPostings){
                i++;
            }
            else if(i+n > nPostings){
                i = nPostings;
            }
            else{
                i+=n;
            }
            nPostings = numPosting[lexicon.get(term).getIndex()];

        }
        // If no such value was found, return a special value indicating that the search failed
        return -1;
    }

    private int getMinDocid(String[] queryTerms) throws IOException {
        int did = nextGEQNew(queryTerms[0], 1);
        for (int i=1; (i<queryTerms.length); i++){
            int d=nextGEQNew(queryTerms[i], did);
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


    //TODO: noi vogliamo aprire in memoria solo i blocchi che ci servono; dobbiamo quindi tenere le due liste decompresse lette
    // e decompresse direttamente dal disco, e quando cambiamo blocco si aggiorna la lista; ci servirà anche un metodo closeList
    // inoltre dobbiamo tenere un iteratore per ogni lista

    private int nextGEQNew(String term, int value) throws IOException {
        //take number of posting in the list and in the blocks
        //check if we are in a new block
        Iterator<Integer> itDocs = docIdsIt[lexicon.get(term).getIndex()];
        Iterator<Integer> itTfs = tfsIt[lexicon.get(term).getIndex()];
        //System.out.println("NextGEQ: " + term + " " + value + " " + lexicon.get(term).getCurdoc() + " " + endDocids[lexicon.get(term).getIndex()]);
        //TODO: we have an error; if the docid is equal to the enddocid, we need to open a new block
        while(lexicon.get(term).getCurdoc() <= endDocids[lexicon.get(term).getIndex()]) { //we need to update the index; check if we are in the last block
            int prec = lexicon.get(term).getCurdoc();
            if(prec >= value) {
                /*if(prec == endDocids[lexicon.get(term).getIndex()]){
                    //System.out.println("BLOCK ENDED!" + value + " " + docId + " " + prec);
                    if(numPosting[lexicon.get(term).getIndex()]+ ConfigurationParameters.SKIP_BLOCK_SIZE >lexicon.get(term).getSkipLen()){
                        //System.out.println("END IN LOOP");
                        return maxDocID;
                    }
                    openListNew(docChannel, tfChannel, skipChannel, term);
                    //itDocs = docIdsIt[lexicon.get(term).getIndex()];
                    //itTfs = tfsIt[lexicon.get(term).getIndex()];
                    int docId = prec;
                    int tf = lexicon.get(term).getCurTf();
                    if(itDocs.hasNext() && itTfs.hasNext()) {
                        docId = itDocs.next();
                        tf = itTfs.next();
                    }
                    lexicon.get(term).setCurdoc(docId);
                    lexicon.get(term).setCurTf(tf);
                }*/
                //System.out.println("RETURNED DOCID: " + prec);
                //System.out.println("NEXT DOCID: " + docId);
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
            if(value >= endDocids[lexicon.get(term).getIndex()] || docId == prec){
                //System.out.println("BLOCK ENDED!" + value + " " + docId + " " + prec);
                if(numPosting[lexicon.get(term).getIndex()]+ ConfigurationParameters.SKIP_BLOCK_SIZE >lexicon.get(term).getSkipLen()){
                    //System.out.println("END IN LOOP");
                    return maxDocID;
                }
                openListNew(docChannel, tfChannel, skipChannel, term);
                itDocs = docIdsIt[lexicon.get(term).getIndex()];
                itTfs = tfsIt[lexicon.get(term).getIndex()];
                //docId = itDocs.next();
                //tf = itTfs.next();
            }
            //System.out.println("DOCID: " + docId);
        }
        // If no such value was found, return a special value indicating that the search failed
        //System.out.println("None found: " + term + " " + value + " " + lexicon.get(term).getCurdoc() + " " + endDocids[lexicon.get(term).getIndex()]);
        return maxDocID;
    }
    public void openListNew(FileChannel docChannel, FileChannel tfChannel, FileChannel skips, String term) throws IOException {
        // Read the compressed posting list data from the file
        //System.out.println("opening new block for " + term);
        Compressor compressor = new Compressor();
        skips.position(lexicon.get(term).getOffsetSkip() + numPosting[lexicon.get(term).getIndex()]);
        ByteBuffer skipInfo = ByteBuffer.allocate(lexicon.get(term).getSkipLen() - numPosting[lexicon.get(term).getIndex()]);
        skips.read(skipInfo);
        skipInfo.position(0);
        int endocid = skipInfo.getInt();
        int skipdocid = skipInfo.getInt();
        int skiptf = skipInfo.getInt();
        endDocids[lexicon.get(term).getIndex()] = endocid;
        //System.out.println("enddocid: " + endocid);
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
        //System.out.println("NEW LIST: " + term + ": " + decompressedDocIds[lexicon.get(term).getIndex()]);
        docIdsIt[lexicon.get(term).getIndex()] = decompressedDocIds[lexicon.get(term).getIndex()].iterator();
        tfsIt[lexicon.get(term).getIndex()] = decompressedTfs[lexicon.get(term).getIndex()].iterator();
        //update offsets
        lexicon.get(term).setOffsetDocid(lexicon.get(term).getOffsetDocid()+skipdocid);
        lexicon.get(term).setOffsetTf(lexicon.get(term).getOffsetTf()+skiptf);
    }

    public CompressedList openList(FileChannel docChannel, FileChannel tfChannel, FileChannel skips, String term) throws IOException {
        docChannel.position(lexicon.get(term).getOffsetDocid());
        // Read the compressed posting list data from the file
        ByteBuffer docIds = ByteBuffer.allocate(lexicon.get(term).getDocidsLen());
        docChannel.read(docIds);
        tfChannel.position(lexicon.get(term).getOffsetTf());
        // Read the compressed posting list data from the file
        ByteBuffer tfs = ByteBuffer.allocate(lexicon.get(term).getTfLen());
        tfChannel.read(tfs);
        skips.position(lexicon.get(term).getOffsetSkip());
        ByteBuffer skipInfo = ByteBuffer.allocate(lexicon.get(term).getSkipLen());
        skips.read(skipInfo);
        skipInfo.position(0);
        docIds.position(0);
        tfs.position(0);
        return new CompressedList(docIds.array(), tfs.array(), skipInfo.array());
    }

}
