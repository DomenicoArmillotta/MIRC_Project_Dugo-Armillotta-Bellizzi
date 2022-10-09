package preprocessing;

import smile.nlp.stemmer.PorterStemmer;
import weka.core.stemmers.SnowballStemmer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Stemmer {
    public List<String> stemming (List<String> list_word) throws IOException {
        PorterStemmer stemmer = new PorterStemmer();
        List<String> stemmed_words = new ArrayList<>();
        for(String word : list_word){
            System.out.println(word);
            word = stemmer.stripPluralParticiple(word);
            word = stemmer.stem(word);
            stemmed_words.add(word);
        }
        return stemmed_words;
    }
}
