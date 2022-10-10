package preprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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

    public List<String> preprocess_doc_optimized (String s) throws IOException {
        //faccio il normalizer
        //poi messo insieme tokenizer , stopword removal e stemmer
        Normalizer normalizer = new Normalizer();
        Stemmer stemmer = new Stemmer();
        //stop words removal
        String path = "docs/stopwords_eng.txt";
        File file_stopwords = new File("docs/stopwords_eng.txt");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        List<String> list_stopwords = Files.readAllLines(p, StandardCharsets.UTF_8);
        //START
        String output_normalizer = normalizer.normalize(s);
        String[] input = output_normalizer.split("\t");
        StringTokenizer st = new StringTokenizer(input[0]);
        List<String> terms = new ArrayList<>();
        //sbagliato da rivedere --> le stop words le devo iterare tutte per forza quindi o(n) sulle stopwords
        boolean stop_finded = false;
        while (st.hasMoreTokens()) {
            String term = st.nextToken();
            stop_finded = false;
            for (int j = 0; j < list_stopwords.size(); j++) {
                if(term.equals(list_stopwords.get(j))){
                    stop_finded = true;

                }
            }
            if(stop_finded == false){
                term = stemmer.stemming_word(term);
                terms.add(term);
            }
        }
        return  terms;
    }




}
