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

    private CompressedList[] postingLists;

    public Daat(){
        lexicon = new HashMap<>();
    }

    /*
    The time complexity of this function is O(n * log(n)), where n is the total number of postings in the heap.
    This is because the time complexity of inserting an element into a heap is O(log(n)), and we insert n elements into the heap.
    Additionally, we perform O(1) operations (such as incrementing variables) for each element in the heap.
    The space complexity of this function is O(n), as we store all n postings in the heap.
    It first preprocesses the input query, and then checks if each term in the query is present in the lexicon. If a term is not present in the lexicon, it returns an empty result list. Otherwise, it retrieves the posting list for the term from the index and adds the postings to a priority queue (heap) in which the posting with the lowest document ID (docid) has the highest priority.
    The method then processes the postings in the heap, one by one, until the heap is empty or the result list is full.
    For each posting, it retrieves the docid and term frequency (tf) from the posting and checks if the docid is different from the current docid. If it is, it resets the count of terms found in the current docid and the current term frequency to 0. It then increments the count and the current term frequency by the tf of the posting.
    Finally, if the count equals the length of the query and the current term frequency is at least k, it adds the docid to the result list. When all the postings have been processed, the method returns the result list.
     */
    public List<Integer> conjunctiveDaat(String query, int k) throws IOException {
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
        TreeMap<Integer, Double> scores = new TreeMap<>();
        int[] tf = new int[proQuery.size()];
        for(String term: proQuery){
            LexiconStats l = getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        postingLists = new CompressedList[queryLen];
        for(int i = 0; i < queryLen; i++){
            postingLists[i] = openList(docChannel, tfChannel, skipChannel, proQuery.get(i));
            lexicon.get(proQuery.get(i)).setIndex(i);
        }
        //TODO: sort query terms in lexicon by shortest list
        int did = 0;
        int maxDocID = (int)ConfigurationParameters.getNumberOfDocuments();
        while (did <= maxDocID){
            did = nextGEQ(docChannel, tfChannel, skipChannel, proQuery.get(0), did);
            //System.out.println("NEW DOCID: " + did);
            if(did == -1){
                break;
            }
            int d = 0;
            for (int i=1; (i<queryLen) && ((d=nextGEQ(docChannel, tfChannel, skipChannel, proQuery.get(i), did)) == did); i++);
            if (d > did){
                //System.out.println("HERE!!! " + did + " " + d);
                did = d; // not in intersection
                //System.out.println("After update!!! " + did + " " + d);
            }
            else
            {
                //docID is in intersection; now get all frequencies
                for (int i=0; i<proQuery.size(); i++){
                    //TODO: update getFreq to compute scores
                    tf[i] = lexicon.get(proQuery.get(i)).getCurTf();
                    System.out.println("tf: " + proQuery.get(i) + " " + did + " " + tf[i]);
                    int docLen = Utils.getDocLen(docIndexChannel, String.valueOf(did));
                    double idf = lexicon.get(proQuery.get(i)).getIdf();
                    //compute BM25 score from frequencies and other data
                    double score = Scorer.bm25Weight(tf[i], docLen, idf); //to modify after getFreq
                    System.out.println(score);
                    //TODO: dopo aver aggiunto uno score al treemap, controllare che ci siano almeno k elementi;
                    // in quel caso togli via il primo
                }
                did++; //and increase did to search for next post
            }
        }
        // Return the result list
        return scores.keySet().stream().collect(Collectors.toList());
    }


    /*
    while pivot < n and current 6= ⊥ do
    10 score ← 0
    11 next ← +∞
    12 for i ← pivot to n − 1 do // Essential lists
    13 if p[i].docid() = current then
    14 score ← score + p[i].score()
    15 p[i].next()
    16 if p[i].docid() < next then
    17 next ← p[i].docid()
    18 for i ← pivot − 1 to 0 do // Non-essential lists
    19 if score + ub[i] ≤ θ then
    20 break
    21 p[i].next(current)
    22 if p[i].docid() = current then
    23 score ← score + p[i].score()
    24 if q.push(current, score) then // List pivot update
    25 θ ← q.min()
    26 while pivot < n and ub[pivot] ≤ θ do
    27 pivot ← pivot + 1
    28 current ← next
     */
    public void disjunctiveDaat(String query, int k) throws IOException {
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
        TreeMap<Integer, Double> scores = new TreeMap<>(); //to store partial scores results
        int[] tf = new int[proQuery.size()];
        TreeMap<String, Double> maxScores = new TreeMap<>(); //to store query term to term upper bound mappings
        double[] termUB = new double[queryLen];
        for(String term: proQuery){
            LexiconStats l = getPointer(lexChannel, term);
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
        for(int i = 0; i < queryLen; i++){
            postingLists[i] = openList(docChannel, tfChannel, skipChannel, queryTerms[i]);
            lexicon.get(queryTerms[i]).setIndex(i);
        }
        HashMap<Integer, Integer> docLens = new HashMap<>();
        int pivot = 0;
        int did = 0;
        did = nextGEQ(docChannel, tfChannel, skipChannel, proQuery.get(0), did);
        double[] documentUB = new double[queryLen];
        double prec = 0.0;
        int index = 0;
        for(double maxscore: termUB){
            documentUB[index] = maxscore + prec;
            prec = documentUB[index];
            System.out.println(queryTerms[index] + " " + documentUB[index]);
            index++;
        }
        double threshold = 0;
        int maxDocID = (int)ConfigurationParameters.getNumberOfDocuments();
        int next = maxDocID;
        for (int i=1; (i<queryLen); i++){
            int d=nextGEQ(docChannel, tfChannel, skipChannel, queryTerms[i], did);
            if(d<did){
                did = d;
            }
        }
        while (pivot < queryLen && did != -1){
            double score = 0;
            //process essential lists
            for (int i=pivot; i<queryLen; i++){
                int current = nextGEQ(docChannel, tfChannel, skipChannel, queryTerms[i], did);
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
                current = nextGEQ(docChannel, tfChannel, skipChannel, queryTerms[i], did+1); //update the pointer to next docid
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
                int current = nextGEQ(docChannel, tfChannel, skipChannel, queryTerms[i], did);
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
            if(scores.firstEntry() == null || scores.firstEntry().getValue() > score){
                scores.put(did, score);
            }
            if(scores.size() > k){
                scores.pollFirstEntry();
            }
            //scores.put(did, score);
            if(scores.firstEntry().getValue() > threshold){
                threshold = scores.firstEntry().getValue();
                System.out.println("updated threshold: " + threshold);
                while(pivot < queryLen && documentUB[pivot]<= threshold){
                    pivot++;
                }
            }
            did = next;
            next++;
            //System.out.println("Next did: " + did);

            //List<String> nonEssential = new ArrayList<>();
            /*for(String key:  maxScores.keySet()){
                score = maxScores.get(key);
                currentScore+=score;
                if(score <= thresholds.peek()){
                    nonEssential.add(key);
                    continue;
                }//the list is essential: process the following lists in pure disjunctive mode
            }*/
            /*did = nextGEQ(docChannel, skipChannel, proQuery.get(0), did);
            if(did == -1){
                break;
            }*/
        }
        //TODO: return top k scores

    }
    //TODO: remove file channel from parameters
    private int nextGEQ(FileChannel docChannel, FileChannel tfChannel, FileChannel skips, String term, int value) throws IOException {
        Compressor c = new Compressor();
        //Seek to the position in the file where the posting list for the term is stored
        int nPostings = lexicon.get(term).getdF();
        int n = (int) Math.floor(Math.sqrt(nPostings));
        int i = 0;
        skips.position(lexicon.get(term).getOffsetSkip());
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
                List<Integer> posting_list = c.variableByteDecodeBlock(blockDocId.array(), n);
                List<Integer> postingTfs = c.unaryDecodeBlock(blockTf.array(), n);
                //System.out.println(term + ": " + postingTfs);
                int index = 0;
                for (int docId : posting_list) {
                    if (docId >= value) {
                        //System.out.println("Found: " + term + " " + docId+ ">=" + value);
                        //lexicon.get(term).setCurdoc(index);
                        lexicon.get(term).setCurTf(postingTfs.get(index));
                        //System.out.println("cur doc: " + term + " " + lexicon.get(term).getCurTf());
                        return docId;
                    }
                    index++;
                }
            }
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

    /*
    Questo è solo uno spunto:
    public List<Document> search(String query, int maxResults) {
      // Set the maximum score to a low initial value
      double maxScore = Double.MIN_VALUE;

      // Create a list to store the results
      List<Document> results = new ArrayList<>();

      // Split the query into individual terms
      String[] terms = query.split(" ");

      // Create a map to store the term upper bounds for each term in the query
      Map<String, Double> termUpperBounds = calculateTermUpperBounds(terms);

      // Set the threshold to a high initial value
      double threshold = Double.MAX_VALUE;

      // Iterate over all documents in the search space
      for (Document doc : searchSpace) {
        // Calculate the score for the current document
        double score = 0.0;

        // Iterate over all terms in the query
        for (String term : terms) {
          // Calculate the contribution of the current term to the score
          double termScore = score(doc, term);

          // If the term score is greater than the term upper bound, set the term score to the upper bound
          if (termScore > termUpperBounds.get(term)) {
            termScore = termUpperBounds.get(term);
          }

          // Add the term score to the overall score
          score += termScore;
        }

        // If the score is higher than the maximum, update the maximum and add the document to the results
        if (score > maxScore) {
          maxScore = score;
          results.add(doc);
        }
        // If the score is lower than the maximum, skip the document
        else {
          continue;
        }

        // If the number of results exceeds the maximum allowed, update the threshold and break the loop if necessary
        if (results.size() >= maxResults) {
          threshold = maxScore;
          if (score < threshold) {
            break;
          }
        }
      }

      return results;
    }
     */

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


    //TODO: questo metodo deve prendere in ingresso l'indice del docid nella lista e chiamare una funzione che decomprime
    // esclusivamente l'elemento desiderato; nella funzione usiamo un contatore, quando abbiamo un elemento, se il contatore
    // non ha raggiunto l'indice, allora andiamo avanti, fino a quando non otteniamo l'elemento per cui il contatore
    // ha raggiunto l'indice corretto
    //iterate over the posting list to get the desired term frequency, return 0 otherwise
    /*private int getFreq(FileChannel tfChannel, String term) throws IOException {
        tfChannel.position(lexicon.get(term).getOffsetTf());
        // Read the compressed posting list data from the file
        ByteBuffer data = ByteBuffer.allocate(lexicon.get(term).getTfLen());
        tfChannel.read(data);
        byte[] tfs = data.array();
        Compressor c = new Compressor();
        int tf = c.unaryDecodeAtPosition(tfs, lexicon.get(term).getCurdoc());
        return tf;
    }*/

    //Aggiungere decompressione
    private int next(RandomAccessFile file, String term, int value) throws IOException {
        //Seek to the position in the file where the posting list for the term is stored
        file.seek(lexicon.get(term).getOffsetDocid());

        // Read the compressed posting list data from the file
        byte[] data = new byte[lexicon.get(term).getDocidsLen()];
        file.read(data);
        // Decompress the data using the appropriate decompression algorithm
        //List<Posting> postingList = decompress(data);
        // Initialize the pointer to the last document processed
        int pointer = 0;
        if(value != -1){
            // Iterate through the posting list to find the index of the last document processed
            /*for(int i = 0; i < postingList.size(); i++){
                if(ByteBuffer.wrap(postingList.get(i).getDocid()).getInt() == value){
                    pointer = i;
                    break;
                }
            }*/
        }
        // Iterate through the posting list starting from the pointer and return the next entry in the list
        /*for(int i = pointer + 1; i < postingList.size(); i++){
            return ByteBuffer.wrap(postingList.get(i).getDocid()).getInt();
        }*/
        // If no such value was found, return a special value indicating that the search failed
        return -1;
    }

    //nextGEQ(lp, k) find the next posting in list lp with docID >= k and
    //return its docID. Return value > MAXDID if none exists.
    private int nextGEQOld(FileChannel docChannel, FileChannel tfChannel, FileChannel skips, String term, int value) throws IOException {
        Compressor c = new Compressor();
        //Seek to the position in the file where the posting list for the term is stored
        docChannel.position(lexicon.get(term).getOffsetDocid());
        // Read the compressed posting list data from the file
        ByteBuffer data = ByteBuffer.allocate(lexicon.get(term).getDocidsLen());
        docChannel.read(data);
        tfChannel.position(lexicon.get(term).getOffsetTf());
        // Read the compressed posting list data from the file
        ByteBuffer tfs = ByteBuffer.allocate(lexicon.get(term).getTfLen());
        tfChannel.read(tfs);
        int nPostings = lexicon.get(term).getdF();
        int n = (int) Math.floor(Math.sqrt(nPostings));
        int i = 0;
        skips.position(lexicon.get(term).getOffsetSkip());
        ByteBuffer readBuffer = ByteBuffer.allocate(lexicon.get(term).getSkipLen());
        skips.read(readBuffer);
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
                List<Integer> posting_list = c.variableByteDecodeBlock(blockDocId.array(), n);
                List<Integer> postingTfs = c.unaryDecodeBlock(blockTf.array(), n);
                //System.out.println(term + ": " + postingTfs);
                int index = 0;
                for (int docId : posting_list) {
                    if (docId >= value) {
                        //System.out.println("Found: " + term + " " + docId+ ">=" + value);
                        //lexicon.get(term).setCurdoc(index);
                        lexicon.get(term).setCurTf(postingTfs.get(index));
                        //System.out.println("cur doc: " + term + " " + lexicon.get(term).getCurTf());
                        return docId;
                    }
                    index++;
                }
            }
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

    /*
    Map the file into memory using the FileChannel.map method. This will return a MappedByteBuffer that you can use to access
    the contents of the file.

    Define a data structure that represents an entry in the file,
    such as a simple Java class with fields for the key and value.

    Implement a comparator for your data structure that can be used to compare two entries based on their keys.

    Begin the binary search by setting the lower bound to 0 and the upper bound to the size of the file, in bytes.

    Iterate until the lower bound is greater than the upper bound. In each iteration, calculate the midpoint
    between the lower and upper bounds and use it to determine the location of the entry in the file.

    Use the MappedByteBuffer.get method to read the entry at the calculated location and construct an instance of
    your data structure from it.

    Use the comparator to compare the key of the entry you just read with the key you're searching for. If they are equal,
    you have found the key and can return the corresponding value. If the key you're searching for is less than the key of the
    entry you just read, set the upper bound to the midpoint - 1. If the key you're searching for is greater than the key of the
    entry you just read, set the lower bound to the midpoint + 1.

    Repeat the process until you have found the key or the lower bound is greater than the upper bound, indicating that the key is
    not present in the file.
     */
    public LexiconStats getPointer(FileChannel channel, String key) throws IOException {
        LexiconStats l = new LexiconStats(); //initialize lexicon stats object
        int entrySize = ConfigurationParameters.LEXICON_ENTRY_SIZE; //take entry size
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size()); //map file to memory
        int lowerBound = 0; //initialize lower bound to the start of the file
        int upperBound = (int) channel.size() - entrySize; //initialize upperbound to the offset of the last entry
        while (lowerBound <= upperBound) {
            int midpoint = (lowerBound + upperBound) / 2; //start from the center
            if(midpoint%entrySize!=0){
                midpoint += midpoint%entrySize; //add the reminder if it's not null
            }
            buffer.position(midpoint);
            ByteBuffer ba = ByteBuffer.allocate(22);
            buffer.get(ba.array(), 0, 22); //take the term bytes
            String value = Text.decode(ba.array());
            value = value.replaceAll("\0", ""); //replace null characters
            if (value.equals(key)) { //if they are equal we are done
                System.out.println("Found key " + key + " at position " + midpoint);
                ByteBuffer bf1 = ByteBuffer.allocate(entrySize-22);
                buffer.get(bf1.array(), 0, entrySize-22); //take the bytes with the information we are searching
                l = new LexiconStats(bf1);
                break;
            } else if (key.compareTo(value) < 0) {
                upperBound = midpoint - entrySize; //we move up if the word comes before
            } else {
                lowerBound = midpoint + entrySize; //we move down if the word comes after
            }
        }
        return l;
    }

    public int getDocLen(FileChannel channel, String key) throws IOException {
        int docLen = 0;
        int entrySize = ConfigurationParameters.DOC_INDEX_ENTRY_SIZE;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
        int lowerBound = 0;
        int upperBound = (int) channel.size()-entrySize;
        while (lowerBound <= upperBound) {
            int midpoint = (lowerBound + upperBound) / 2;
            if(midpoint%entrySize!=0){
                midpoint += midpoint%entrySize;
            }
            buffer.position(midpoint);
            ByteBuffer ba = ByteBuffer.allocate(10);
            buffer.get(ba.array(), 0, 10);
            String value = Text.decode(ba.array());
            value = value.replaceAll("\0", "");
            if (value.equals(key)) {
                System.out.println("Found key " + key + " at position " + midpoint);
                ByteBuffer bf1 = ByteBuffer.allocate(4);
                buffer.get(bf1.array(), 0, 4);
                docLen = bf1.getInt();
                break;
            } else if (Integer.parseInt(key) - Integer.parseInt(value) < 0) {
                upperBound = midpoint - entrySize;
            } else {
                lowerBound = midpoint + entrySize;
            }
        }
        return docLen;
    }

}
