package inverted_index;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.IntStream;

public class Inverted_index{

    private Hashtable<String,Integer> dict;
    private String outputFile;

    private HashMap<String, List<Posting>> index;


    public Inverted_index() {
        dict = new Hashtable<>();
        index = new HashMap();
    }

    //TODO 12/10/2022: questo metodo è inutile, appena confermato che non serve cancellare il metodo!
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

    public List<Posting> getPostings(String term){
        List<Posting> postingList= new LinkedList<>();
        postingList = index.get(term);
        return postingList;
    }

    public void addPosting(String term, int docid, int freq, int pos){
        if(index.get(term) == null){
            List<Posting> l = new LinkedList<>();
            l.add(new Posting(docid, freq, pos));
            index.put(term,l);
        }
        else{
            List<Posting> l = index.get(term);
            for(Posting posting: l){
                if(posting.getDocumentId() == docid){
                    posting.addOccurrence();
                    posting.addPos(pos);
                    return;
                }
            }
            index.get(term).add(new Posting(docid,freq, pos));
        }
    }

    public void addToDict(String term){
         if(dict.containsKey(term)){
             dict.put(term, dict.get(term) + 1);
         }else{
             dict.put(term , 1);
         }
    }

    public Set<String> getTerms(){
        Set<String> terms = index.keySet();
        return terms;
    }

    //TODO 13/10/2022: add the sorting of the dictionary
    public void sortPosting() {
        for(List<Posting> postingList : index.values()){
            Collections.sort(postingList);
        }
    }

    //TODO 11/10/2022: il metodo non serve
    public void mergePostings(int n) {
        for (Map.Entry d: dict.entrySet()){
            String term = (String) d.getKey();
            //writeToDisk(term,n);
        }
    }

    public void writeToDisk(int n){
        writeDict(n);
        writeDocids(n);
        writePositions(n);
        writeFrequency(n);
        //devo scrivere ogni elemento della posting list in un file separato a seconda dell'elemento
        /*int size = l.size();
        int[] docids = new int[size];
        String[] positions = new String[size];
        int freqsum = 0;
        int i = 0;
        for(Posting p:  l){
            if(IntStream.of(docids).anyMatch(x -> x == p.getDocumentId())){
                docids[i] = p.getDocumentId();
            }
            positions[i] += p.getPos() + ", ";
            freqsum += p.getTermFrequency();
            i++;
        }
        writeDocids(docids,n);
        writePositions(positions,n);
        writeFrequency(freqsum,n);*/

    }

   /* public void writeToFile(int nIndex){
        BufferedWriter bf = null;
        String outputFilePath = "docs/inverted_index_test"+nIndex+".txt";
        File file = new File(outputFilePath);

        try {

            // create new BufferedWriter for the output file
            bf = new BufferedWriter(new FileWriter(file));

            // iterate map entries
            for (Map.Entry<String, List<Posting>> entry :
                    index.entrySet()) {

                // put key and value separated by a colon
                //bf.write(entry.getKey() + ":" + entry.getValue());
                bf.write(entry.getValue().toString()); //write the posting lists
                // new line
                bf.newLine();
            }

            bf.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {

                // always close the writer
                bf.close();
            }
            catch (Exception e) {
            }
        }
    }*/

    //TODO 13/10/2022: controllare se va bene fare le scritture così e testarle!

    public void writeDict(int n){
        BufferedWriter bf = null;
        String outputFilePath = "docs/lexicon_"+n+".txt";
        File file = new File(outputFilePath);
        try {
            // create new BufferedWriter for the output file
            bf = new BufferedWriter(new FileWriter(file));
            for(String term : dict.keySet()) {
                bf.write(term); //write the docids for a term
                // new line
                bf.newLine();
            }
            bf.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {
                // always close the writer
                bf.close();
            }
            catch (Exception e) {
            }
        }
    }

    public void writeDocids(int n){
        BufferedWriter bf = null;
        String outputFilePath = "docs/inverted_index_docids_"+n+".txt";
        File file = new File(outputFilePath);
        try {
            // create new BufferedWriter for the output file
            bf = new BufferedWriter(new FileWriter(file));
            for(List<Posting> postingList : index.values()){
                String docIds = "";
                for(Posting p : postingList){
                    docIds += p.getDocumentId() + " ";
                }
                bf.write(docIds); //write the docids for a term
                // new line
                bf.newLine();
            }
            bf.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {
                // always close the writer
                bf.close();
            }
            catch (Exception e) {
            }
        }
    }

    public void writePositions(int n){
        BufferedWriter bf = null;
        String outputFilePath = "docs/inverted_index_positions_"+n+".txt";
        File file = new File(outputFilePath);

        try {

            // create new BufferedWriter for the output file
            bf = new BufferedWriter(new FileWriter(file));
            for(List<Posting> postingList : index.values()){
                String positions = "";
                for(Posting p : postingList){
                    positions += p.getPos().toString();
                }
                bf.write(positions); //write the positions for each term
                // new line
                bf.newLine();
            }
            bf.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {
                // always close the writer
                bf.close();
            }
            catch (Exception e) {
            }
        }
    }

    public void writeFrequency(int n){
        BufferedWriter bf = null;
        String outputFilePath = "docs/inverted_index_term_freq_"+n+".txt";
        File file = new File(outputFilePath);

        try {

            // create new BufferedWriter for the output file
            bf = new BufferedWriter(new FileWriter(file));
            for(List<Posting> postingList : index.values()){
                String freq = "";
                for(Posting p : postingList){
                    freq += p.getTermFrequency();
                }
                bf.write(freq); //write the frequency for a term
                // new line
                bf.newLine();
            }
            bf.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {
                // always close the writer
                bf.close();
            }
            catch (Exception e) {
            }
        }
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

    public HashMap<String, List<Posting>> getIndex() {
        return index;
    }

    public void setIndex(HashMap<String, List<Posting>> index) {
        this.index = index;
    }

}

