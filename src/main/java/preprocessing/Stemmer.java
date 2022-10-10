package preprocessing;
//we use the standard library 'snowball' for porter stemming of eng, to use it we imported the dependency
import smile.nlp.stemmer.PorterStemmer;
import weka.core.stemmers.SnowballStemmer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Stemmer {
    /**
     * takes an already tokenized word list as input and returns the stemmed word list
     * @param list_word tokenized list of word
     * @return stemmed_words from list_word
     * @throws IOException
     */
    public List<String> stemming (List<String> list_word) throws IOException {
        PorterStemmer stemmer = new PorterStemmer();
        List<String> stemmed_words = new ArrayList<>();
        for(String word : list_word){
            word = stemmer.stripPluralParticiple(word);
            word = stemmer.stem(word);
            stemmed_words.add(word);
        }
        return stemmed_words;
    }

    /**
     * takes a token as input and returns the stemmed token
     * we use this function for the optimised pre-processing pipeline
     * @param word input
     * @return word output
     * @throws IOException
     */
    public String stemming_word (String word) throws IOException {
        PorterStemmer stemmer = new PorterStemmer();
        word = stemmer.stripPluralParticiple(word);
        word = stemmer.stem(word);
        return word;
    }

}
