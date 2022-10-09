package preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Preprocess_doc {
    public List<String> preprocess_doc (String s) throws IOException {
        Normalizer normalizer = new Normalizer();
        Tokenizer tokenizer = new Tokenizer();
        Stopword_removal stopword_removal = new Stopword_removal();
        String output_normalizer = normalizer.normalize(s);
        List<String> output_tokenizer = new ArrayList<>();
        output_tokenizer = tokenizer.tokenize(output_normalizer);
        List<String> output_stopwords_removal = new ArrayList<>();
        output_stopwords_removal = stopword_removal.remove(output_tokenizer);
        return output_stopwords_removal;
    }


}
