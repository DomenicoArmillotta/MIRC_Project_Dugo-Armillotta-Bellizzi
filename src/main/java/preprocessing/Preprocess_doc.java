package preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Preprocess_doc {
    /**
     * given a string, runs the preprocessing pipeline and returns a processed word list.
     * In order it performs: tokenization , stopword removal , stemming
     * @param s string of document to elaborate
     * @return output_stopwords_removal
     * @throws IOException
     */
    public List<String> preprocess_doc (String s) throws IOException {
        Normalizer normalizer = new Normalizer();
        Tokenizer tokenizer = new Tokenizer();
        Stopword_removal stopword_removal = new Stopword_removal();
        Stemmer stemmer = new Stemmer();
        String output_normalizer = normalizer.normalize(s);
        List<String> output_tokenizer = new ArrayList<>();
        output_tokenizer = tokenizer.tokenize(output_normalizer);
        List<String> output_stopwords_removal = new ArrayList<>();
        output_stopwords_removal = stopword_removal.remove(output_tokenizer);
        List<String> output_stemming = new ArrayList<>();
        output_stemming = stemmer.stemming(output_stopwords_removal);
        return output_stemming;
    }


}
