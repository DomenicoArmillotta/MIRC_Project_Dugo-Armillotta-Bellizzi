package queryProcessing;

import invertedIndex.InvertedIndex;
import invertedIndex.LexiconStats;
import invertedIndex.Posting;
import org.apache.hadoop.io.Text;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import preprocessing.PreprocessDoc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;


public class Daat {

    private int maxDocID;

    private HashMap<String, LexiconStats> lexicon;
    private DB db;
    private HTreeMap<String, Integer> docIndex;
    private InvertedIndex index;
    private String lexiconPath = "docs/lexicon.txt";
    private String docidsPath = "docs/docids.txt";
    private String tfsPath = "docs/tfs.txt";


    public Daat(){
        //open document index
        db = DBMaker.fileDB("docs/docIndex.db").make();
        docIndex = db
                .hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
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
        // Initialize the result list
        List<Integer> result = new ArrayList<>();
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        FileChannel lexChannel = lexFile.getChannel();
        MappedByteBuffer lexBuf = lexChannel.map(FileChannel.MapMode.READ_WRITE,0, lexChannel.size());
        // Create a heap to store the postings of the terms in the query
        /*PriorityQueue<Posting> heap = new PriorityQueue<>();
        for (String term : proQuery) {
            // Check if the term is in the lexicon
            if (lexicon.containsKey(term)) {
                // Get the posting list for the term
                List<Posting> pl = index.get(lexicon.get(term).getIndex());
                // Convert the byte array to a list of Posting objects
                List<Posting> postings = new ArrayList<>();
                    // Check if the term is in the lexicon
                    if (lexicon.containsKey(term)) {
                        // Get the posting list for the term
                        // Add the postings to the heap
                        heap.addAll(pl);
                    } else {
                        // If the term is not in the lexicon, return an empty result list
                        return result;
                    }
                
                // Add the postings to the heap
                heap.addAll(postings);
            } else {
                // If the term is not in the lexicon, return an empty result list
                return result;
            }
        }
        // Initialize the current docid and term frequency
        int curDocid = -1;
        int curTf = 0;
        // Initialize the count of the terms found in the current docid
        int count = 0;
        // Process the postings in the heap until it is empty or the result list is full
        while (!heap.isEmpty() && result.size() < k) {
            Posting posting = heap.poll();
            // Get the docid and term frequency from the posting
            int docid = ByteBuffer.wrap(posting.getDoc()).getInt();
            int tf = ByteBuffer.wrap(posting.getTf()).getInt();
            if (docid != curDocid) {
                // If the docid is different from the current docid, reset the count
                count = 0;
                curDocid = docid;
                curTf = 0;
            }
            // Increment the count and the term frequency
            count++;
            curTf += tf;
            if (count == queryLen && curTf >= k) {
                // If all the terms in the query are found and the term frequency is at least k, add the docid to the result list
                result.add(docid);
            }
        }*/
        // Return the result list
        return result;
    }


    public void disjunctiveDaat(String query, int k) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> proQuery = new ArrayList<>();
        proQuery = preprocessing.preprocess_doc_optimized(query);
        int queryLen = proQuery.size();
        //TODO: complete --> bisogna implementare maxscore:
        // vedere se fare term upper bound

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



    public ArrayList<Posting> openList(String queryTerm) throws IOException {
        //TODO: implement
        return null;
    }

    //iterate over the posting list ot get the desired term frequency, return 0 otherwise
    private int getFreq(ArrayList<Posting> postingList, int docid){
        int pointer = 0;
        while(pointer < postingList.size()){
            Posting p = postingList.get(pointer);
            if(ByteBuffer.wrap(p.getDocid()).getInt() == docid){
                return ByteBuffer.wrap(p.getTf()).getInt();
            }
            pointer++;
        }
        return 0;
    }


    /*private int getFreq(ArrayList<Posting> postingList, int docid){
        for(Posting p: postingList){
            if(p.getDocumentId() == docid) return p.getTermFrequency();
        }
        return 0;
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
    private int nextGEQ(RandomAccessFile file, String term, int value) throws IOException {
        //TODO: implement
        //Seek to the position in the file where the posting list for the term is stored
        file.seek(lexicon.get(term).getOffsetDocid());

        // Read the compressed posting list data from the file
        byte[] data = new byte[lexicon.get(term).getDocidsLen()];
        file.read(data);

        // Decompress the data using the appropriate decompression algorithm
        //List<Integer> posting_list = decompress(data);

        // Iterate through the posting list and return the first entry that is greater than or equal to the search value
        /*for (int doc_id : posting_list) {
            if (doc_id >= value) {
                return doc_id;
            }
        }*/

        // If no such value was found, return a special value indicating that the search failed
        return -1;
    }

    //computes the average document length over the whole collection
    /*private double averageDocumentLength(){
        double avg = 0;
        for(int len: htDocindex.values()){
            avg+= len;
        }
        return avg/ htDocindex.keySet().size();
    }*/

    //TODO: serve un metodo per cercare un termine nel file del lexicon (binary search)
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
        LexiconStats l = new LexiconStats();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
        int lowerBound = 0;
        int upperBound = (int) channel.size() - 4;
        while (lowerBound <= upperBound) {
            int midpoint = (lowerBound + upperBound) / 2;
            buffer.position(midpoint);
            byte[] term = buffer.get(buffer.array(), 0, 22).array();
            String value = Text.decode(term).toString();
            if (value == key) {
                System.out.println("Found key " + key + " at position " + midpoint);
                //TODO: read the value in a bytebuffer
                break;
            } else if (key.compareTo(value) < 0) {
                upperBound = midpoint - 4;
            } else {
                lowerBound = midpoint + 4;
            }
        }
        //TODO: set the value in the lexiconstats object
        return l;
    }

}
