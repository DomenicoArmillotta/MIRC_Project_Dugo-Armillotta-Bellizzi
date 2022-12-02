package inverted_index;

import org.mapdb.DB;
import org.mapdb.HTreeMap;

import java.nio.channels.FileChannel;

public class InvertedIndex {

    private DB db;
    private FileChannel fc;
    private String outPath;
    private HTreeMap<String, LexiconStats> lexicon;

    public InvertedIndex(int n){

    }

    public void addPosting(String term, int docid, int freq){
    }

    public void addToLexicon(String term){
    }

    public void sortTerms() {
    }

    public void writePostings(){
    }

}

