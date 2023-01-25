package queryProcessing;

import compression.Compressor;
import fileManager.ConfigurationParameters;
import invertedIndex.CompressedList;
import invertedIndex.InvertedIndex;
import invertedIndex.LexiconStats;
import invertedIndex.Posting;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.C;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import preprocessing.PreprocessDoc;
import utility.Utils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;


public class Daat {

    private int maxDocID;

    public HashMap<String, LexiconStats> lexicon;
    private InvertedIndex index;
    private String lexiconPath = "docs/lexiconTot.txt";
    private String docidsPath = "docs/docids.txt";
    private String tfsPath = "docs/tfs.txt";
    private String docIndexPath = "docs/docIndex.txt";
    private String skipsPath = "docs/skipInfo.txt";

    private CompressedList[] postingLists; //to keep the compressed posting list for each term of the query

    private List<Integer>[] decompressedDocIds; //to keep the current block of docids for each term of the query
    private List<Integer>[] decompressedTfs; //to keep the current block of tfs for each term of the query

    public Daat(){
        lexicon = new HashMap<>();
    }


    public List<Map.Entry<Integer,Double>> conjunctiveDaat(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = new ArrayList<>();
        proQuery = preprocessing.preprocess_doc_optimized(query);
        int queryLen = proQuery.size();
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        FileChannel lexChannel = lexFile.getChannel();
        RandomAccessFile docFile = new RandomAccessFile(new File(docidsPath), "rw");
        FileChannel docChannel = docFile.getChannel();
        RandomAccessFile tfFile = new RandomAccessFile(new File(tfsPath), "rw");
        FileChannel tfChannel = tfFile.getChannel();
        RandomAccessFile skipFile = new RandomAccessFile(new File(skipsPath), "rw");
        FileChannel skipChannel = skipFile.getChannel();
        RandomAccessFile docIndexFile = new RandomAccessFile(new File(docIndexPath), "rw");
        FileChannel docIndexChannel = docIndexFile.getChannel();
        postingLists = new CompressedList[queryLen];
        decompressedDocIds = new List[queryLen];
        decompressedTfs = new List[queryLen];
        TreeMap<Integer, Double> scores = new TreeMap<>();
        int[] tf = new int[proQuery.size()];
        for(String term: proQuery){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        postingLists = new CompressedList[queryLen];
        for(int i = 0; i < queryLen; i++){
            postingLists[i] = openList(docChannel, tfChannel, skipChannel, proQuery.get(i));
            lexicon.get(proQuery.get(i)).setIndex(i);
        }
        //TODO: sort query terms in lexicon by shortest list
        int did = 0;
        maxDocID = (int)ConfigurationParameters.getNumberOfDocuments();
        while (did <= maxDocID){
            double score = 0.0;
            did = nextGEQ(proQuery.get(0), did);
            //System.out.println("NEW DOCID: " + did);
            if(did == -1){
                break;
            }
            int d = 0;
            for (int i=1; (i<queryLen) && ((d=nextGEQ(proQuery.get(i), did)) == did); i++);
            if (d > did){
                //System.out.println("HERE!!! " + did + " " + d);
                did = d; // not in intersection
                //System.out.println("After update!!! " + did + " " + d);
            }
            else
            {
                //docID is in intersection; now get all frequencies
                for (int i=0; i<proQuery.size(); i++){
                    tf[i] = lexicon.get(proQuery.get(i)).getCurTf();
                    //System.out.println("tf: " + proQuery.get(i) + " " + did + " " + tf[i]);
                    int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    double idf = lexicon.get(proQuery.get(i)).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.bm25Weight(tf[i], docLen, idf); //to modify after getFreq
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
        List<String> proQuery = preprocessing.preprocess_doc_optimized(query);
        int queryLen = proQuery.size();
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        FileChannel lexChannel = lexFile.getChannel();
        RandomAccessFile docFile = new RandomAccessFile(new File(docidsPath), "rw");
        FileChannel docChannel = docFile.getChannel();
        RandomAccessFile tfFile = new RandomAccessFile(new File(tfsPath), "rw");
        FileChannel tfChannel = tfFile.getChannel();
        RandomAccessFile skipFile = new RandomAccessFile(new File(skipsPath), "rw");
        FileChannel skipChannel = skipFile.getChannel();
        RandomAccessFile docIndexFile = new RandomAccessFile(new File(docIndexPath), "rw");
        FileChannel docIndexChannel = docIndexFile.getChannel();
        TreeMap<Integer, Double> scores = new TreeMap<>(); //to store partial scores results
        int[] tf = new int[proQuery.size()];
        TreeMap<String, Double> maxScores = new TreeMap<>(); //to store query term to term upper bound mappings
        double[] termUB = new double[queryLen];
        for(String term: proQuery){
            LexiconStats l = Utils.getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        for(int i = 0; i < queryLen; i++){
            termUB[i] = lexicon.get(proQuery.get(i)).getTermUpperBound();
        }
        for(String term: proQuery){
            maxScores.put(term, lexicon.get(term).getTermUpperBound());
        }
        HashMap<String, Double> sortedTerms = new HashMap();
        maxScores.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue())
                .forEachOrdered(x -> sortedTerms.put(x.getKey(), x.getValue()));
        Arrays.sort(termUB);
        String [] queryTerms = sortedTerms.keySet().toArray(new String[0]);
        postingLists = new CompressedList[queryLen];
        decompressedDocIds = new List[queryLen];
        decompressedTfs = new List[queryLen];
        for(int i = 0; i < queryLen; i++){
            postingLists[i] = openList(docChannel, tfChannel, skipChannel, queryTerms[i]);
            lexicon.get(queryTerms[i]).setIndex(i);
        }
        HashMap<Integer, Integer> docLens = new HashMap<>();
        int pivot = 0;
        int did = 0;
        did = nextGEQ(queryTerms[0], did);
        //lexicon.get(queryTerms[0]).setCurdoc(did);
        double[] documentUB = new double[queryLen];
        double prec = 0.0;
        int index = 0;
        for(double maxscore: termUB){
            documentUB[index] = maxscore + prec;
            prec = documentUB[index];
            //System.out.println(queryTerms[index] + " " + documentUB[index]);
            index++;
        }
        double threshold = 0;
        maxDocID = (int)ConfigurationParameters.getNumberOfDocuments();
        int next;
        for (int i=1; (i<queryLen); i++){
            int d=nextGEQ(queryTerms[i], did);
            //lexicon.get(queryTerms[i]).setCurdoc(d);
            if(d<did){
                did = d;
            }
        }
        while (pivot < queryLen && did != -1){
            next = maxDocID;
            double score = 0.0;
            //process essential lists
            for (int i=pivot; i<queryLen; i++){
                //int current = lexicon.get(queryTerms[i]).getCurdoc();
                int current = nextGEQ(queryTerms[i], did);
                if(current == did){
                    tf[i] = lexicon.get(queryTerms[i]).getCurTf();
                    //int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    int docLen;
                    if(docLens.get(did) == null){
                        docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    }
                    else{
                        docLen = docLens.get(did);
                    }
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.bm25Weight(tf[i], docLen, idf);
                    //System.out.println("Partial score for " + queryTerms[i] + " " + did + ": " + score);
                }
                current = nextGEQ(queryTerms[i], did+1); //update the pointer to next docid
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
                    tf[i] = lexicon.get(queryTerms[i]).getCurTf();
                    //int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    int docLen;
                    if(docLens.get(did) == null){
                        docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    }
                    else{
                        docLen = docLens.get(did);
                    }
                    double idf = lexicon.get(queryTerms[i]).getIdf();
                    //compute BM25 score from frequencies and other data
                    score += Scorer.bm25Weight(tf[i], docLen, idf);
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
                if(minEntry.getValue() <= score){
                    scores.put(did, score);
                }
            }
            if(scores.size() > k){
                scores.remove(minEntry.getKey(), minEntry.getValue());
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
    //TODO: remove file channel from parameters
    private int nextGEQ(String term, int value) throws IOException {
        Compressor c = new Compressor();
        //take number of posting in the list and in the blocks
        int nPostings = lexicon.get(term).getdF();
        int n = (int) Math.floor(Math.sqrt(nPostings));
        int i = 0;
        ByteBuffer data = ByteBuffer.wrap(postingLists[lexicon.get(term).getIndex()].getDocids());
        ByteBuffer readBuffer = ByteBuffer.wrap(postingLists[lexicon.get(term).getIndex()].getSkipInfo());
        ByteBuffer tfs = ByteBuffer.wrap(postingLists[lexicon.get(term).getIndex()].getTfs());
        readBuffer.position(0);
        data.position(0);
        tfs.position(0);
        //System.out.println("NextGEQ: " + term + " " + value + " " + nPostings + " " + n);
        //System.out.println("TFS: "+ c.unaryDecode(tfs.array()));
        while(i< nPostings) {
            //we need to update the index; check if we are in the last block
            int endocid = readBuffer.getInt();
            int skipdocid = readBuffer.getInt();
            int skiptf = readBuffer.getInt();
            ByteBuffer blockDocId = ByteBuffer.allocate(skipdocid);
            //System.out.println("Skipping: " + term + " " + value + " " + endocid + " " + skipdocid + " " + skiptf + " " + data.array().length);
            data.get(blockDocId.array(), 0, skipdocid);
            ByteBuffer blockTf = ByteBuffer.allocate(skipdocid);
            tfs.get(blockTf.array(), 0, skiptf);
            if(endocid >= value) {
                //System.out.println("Skipped: " + term + " " + endocid + ">=" + value);
                //we do this to decompress only one time when we first enter the block, and not each time we enter
                if(decompressedDocIds[lexicon.get(term).getIndex()] == null){
                    decompressedDocIds[lexicon.get(term).getIndex()] = c.variableByteDecodeBlock(blockDocId.array(), n);
                    decompressedTfs[lexicon.get(term).getIndex()] = c.unaryDecodeBlock(blockTf.array(), n);
                }
                //List<Integer> postingDocIds = c.variableByteDecodeBlock(blockDocId.array(), n);
                //List<Integer> postingTfs = c.unaryDecodeBlock(blockTf.array(), n);
                //System.out.println(term + ": " + postingTfs);
                int index = 0;
                for (int docId : decompressedDocIds[lexicon.get(term).getIndex()]) {
                    if (docId >= value) {
                        //System.out.println("Found: " + term + " " + docId+ ">=" + value);
                        lexicon.get(term).setCurTf(decompressedTfs[lexicon.get(term).getIndex()].get(index));
                        //System.out.println("cur doc: " + term + " " + " " + docId + " " + lexicon.get(term).getCurTf());
                        return docId;
                    }
                    index++;
                }
            }
            //update posting lists removing the old block
            postingLists[lexicon.get(term).getIndex()] = new CompressedList(data.array(), tfs.array(), readBuffer.array());
            decompressedDocIds[lexicon.get(term).getIndex()] = null;
            decompressedTfs[lexicon.get(term).getIndex()] = null;
            //System.out.println("Update index "+ term + " " + value + " " + i + " " + n + " " + nPostings);
            if(i+1 == nPostings){
                i++;
            }
            else if(i+n > nPostings){
                i = nPostings;
            }
            else{
                i+=n;
            }

        }
        // If no such value was found, return a special value indicating that the search failed
        return -1;
    }

    //TODO: fare trec eval!!!!!


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
