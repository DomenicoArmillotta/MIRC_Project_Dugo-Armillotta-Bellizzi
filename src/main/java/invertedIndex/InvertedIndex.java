package invertedIndex;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.nio.channels.FileChannel;
import java.util.List;

public class InvertedIndex {

    private DB db;
    private String outPath;
    private HTreeMap<String, LexiconStats> lexicon;

    public InvertedIndex(int n){
        db = DBMaker.fileDB("docs/index"+n+".db").make();
        lexicon = db.hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();

    }

    public void addPosting(String term, int docid, int freq){
        List<Posting> pl = (List<Posting>) db.indexTreeList(term, Serializer.JAVA).createOrOpen();
        if(lexicon.get(term) != null){
            for (int i = 0; i < pl.size(); i++) {
                if (pl.get(i).getDocumentId() == docid) {
                    int newTf = pl.get(i).getTermFrequency()+1;
                    pl.remove(i);
                    pl.add(new Posting(docid,newTf));
                    return;
                }
            }
        }
        pl.add(new Posting(docid, 1));
    }

    public void addToLexicon(String term){
        lexicon.put(term, new LexiconStats());
    }

    public void sortTerms() {
        //lexicon = lexicon.entrySet().stream().sorted();
    }

    public void writePostings(){
        //TODO: declare FileChannel for three different files: one for docids, one for tfs, one for lexicon
    }

}

