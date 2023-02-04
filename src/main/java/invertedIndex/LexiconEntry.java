package invertedIndex;

/**
 * Single lexicon entry, there is an entry for each term in our collection.
 * Each entry consists of the Term-LexiconStats tuple
 */
public class LexiconEntry {
    private String term; //term of the lexicon
    private LexiconStats lexiconStats; //pointers for that term

    public LexiconEntry(String term, LexiconStats lexiconStats){
        this.term = term;
        this.lexiconStats = lexiconStats;
    }
    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public LexiconStats getLexiconStats() {
        return lexiconStats;
    }

    public void setLexiconStats(LexiconStats lexiconStats) {
        this.lexiconStats = lexiconStats;
    }
}
