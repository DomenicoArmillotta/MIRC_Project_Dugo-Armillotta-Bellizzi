package inverted_index;

import java.io.*;
import java.util.*;

public class InvertedIndex {

    private Hashtable<String,Integer> dict;
    private String outputFile;

    private HashMap<String, List<Posting>> index;

    private TreeMap<String, Integer> sortedDict;
    private TreeMap<String, List<Posting>> sortedIndex;


    public InvertedIndex() {
        dict = new Hashtable<>();
        index = new HashMap();
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

    public void sortPosting() {
        TreeMap<String, Integer> tmd = new TreeMap<>(dict);
        sortedDict = tmd;
        //System.out.println(sortedDict);
        for(List<Posting> postingList : index.values()){
            Collections.sort(postingList);
        }
        TreeMap<String, List<Posting>> tmi = new TreeMap<>(index);
        sortedIndex = tmi;
    }

    public void writeToDisk(int n){
        writeDict(n);
        writeDocids(n);
        writePositions(n);
        writeFrequency(n);
    }

    //TODO 17/10/2022: add the offset of the posting list to the dictionary!
    public void writeDict(int n){
        BufferedWriter bf = null;
        String outputFilePath = "docs/lexicon_"+n+".txt";
        File file = new File(outputFilePath);
        try {
            // create new BufferedWriter for the output file
            bf = new BufferedWriter(new FileWriter(file));
            int cont = 0;
            for(String term : sortedDict.keySet()) {
                term += " " + cont;
                bf.write(term); //write the docids for a term
                // new line
                bf.newLine();
                cont++;
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
            for(List<Posting> postingList : sortedIndex.values()){
                String docIds = "";
                if(postingList.size() == 1){
                    docIds += postingList.get(0).getDocumentId();
                }
                else{
                    for(Posting p : postingList){
                        docIds += p.getDocumentId() + " ";
                    }
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
            for(List<Posting> postingList : sortedIndex.values()){
                String positions = "";
                if(postingList.size() == 1){
                    positions = postingList.get(0).getPositionString();
                }
                else{
                    for(Posting p : postingList){
                        positions += p.getPositionString() + " ";
                        //positions += p.getPos().toString() + " ";
                    }
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
            for(List<Posting> postingList : sortedIndex.values()){
                String freq = "";
                if(postingList.size() == 1){
                    freq += postingList.get(0).getTermFrequency();
                }
                else{
                    for(Posting p : postingList){
                        freq += p.getTermFrequency() + " ";
                    }
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

