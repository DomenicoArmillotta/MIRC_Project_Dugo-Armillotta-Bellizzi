package inverted_index;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Inverted_index{

    private Hashtable<String,Integer> dict;
    private String outputFile;



    public Inverted_index() {
        dict = new Hashtable<>();
    }



    HashMap<String, List<Posting>> index = new HashMap();
    public static void createInvertedIndex(String path) throws IOException {
        Preprocess_doc preprocessing = new Preprocess_doc();
        File file = new File(path);
        Path p = Paths.get(path);
        List<String> list_doc = new ArrayList<>();
        // to not read all the documents in memory we use a LineIterator
        LineIterator it = FileUtils.lineIterator(file, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                list_doc.add(line);
            }
        } finally {
            LineIterator.closeQuietly(it);
        }

        for (int i = 0; i < list_doc.size(); i++) {
            String current_doc = list_doc.get(i);
            String[] parts = current_doc.split("\t");
            int doc_id = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> pro_doc = new ArrayList<>();
            pro_doc = preprocessing.preprocess_doc_optimized(doc_corpus);
            long curTime = System.currentTimeMillis();
            final TreeMap<String, ArrayList<String>> dictionary = new TreeMap<String, ArrayList<String>>();
            // Fill the dictionary.
            /*for (final String tokens : pro_doc) {
                if (dictionary.get(parts[0]) == null) {
                    dictionary.put(parts[0], new ArrayList<String>());
                }
                dictionary.get(parts[0]).add(parts[1]);
            }*/

            System.out.println("Sorted in " + ((System.currentTimeMillis() - curTime) / 1000) + " seconds.");
        /*

        Parse the document and read the docs

         */

       /* // Write the block to disk.
        curTime = System.currentTimeMillis();
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                 ".txt")));
        for (final Map.Entry<String, ArrayList<String>> entry : dictionary.entrySet()) {
            writer.append(entry.getKey() + "=");
            for (int i = 0; i < entry.getValue().size() - 1; i++)
                writer.append(entry.getValue().get(i) + ",");
            writer.append(entry.getValue().get(entry.getValue().size() - 1));
            writer.append("\n");
        }
        writer.close();
        System.out.println("Block created in " + ((System.currentTimeMillis() - curTime) / 1000) + " seconds.");*/
        }
    }

    //TODO: define the size of the blocks, depending on the #calls of SPIMI we want to do (SPIMI complexity is O(T)!!) We start with 10 blocks
    
    public List<Posting> getPostings(String term){
        List<Posting> postingList= new LinkedList<>();
        postingList = index.get(term);
        return postingList;
    }

    public void addPosting(String term, int docid, int freq){
        if(index.get(term) == null){
            List<Posting> l = new LinkedList<>();
            l.add(new Posting(docid, freq));
            index.put(term,l);
        }
        else{
            index.get(term).add(new Posting(docid,freq));
        }
    }

    public Set<String> getTerms(){
        Set<String> terms = index.keySet();
        return terms;
    }

    public Map<String, List<Posting>> sortPosting(HashMap<String, List<Posting>> index) {
        Map<String, List<Posting>> sorted = new TreeMap;
        sorted.putAll(index);
        for (int i=0; i<sorted.size(); i++){
            List<Posting> values = Collections.sort(sorted.get(i));
        }

        return sorted;
    }

    public int mergePostings(int doc_id, Map<Integer, Integer> control) {
        int countingNewEntries = 0;
        for(Map.Entry entry : control.entrySet()) {
            String term = (String)entry.getKey();
            int tf = (Short)entry.getValue();
            Posting posting = new Posting(doc_id, tf);

            //controllo se ci sono posting lists per il termine
            LinkedList<Posting> postingsList = (LinkedList<Posting>) index.get(term);
            if (postingsList == null) {
                //inserisci nuovo termine e incrementa contatore
                postingsList = new LinkedList<>();
                index.put(term, postingsList);
                countingNewEntries++;
            }
            postingsList.add(posting);
        }
        return countingNewEntries;
    }

    public String getKey(String term) {
        int hash = Math.abs(term.hashCode()); //prendo il valore assoluto
        return Integer.toString(hash);
    }


    public Hashtable<String, Integer> getDict() {
        return dict;
    }

    public void setDict(Hashtable<String, Integer> dict) {
        this.dict = dict;
    }

    public String getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(String outputFile) {
        this.outputFile = outputFile;
    }

    //TODO: merge method, write to file method, dictionary for each block, get terms, sort posting lists by increasing docid


}

