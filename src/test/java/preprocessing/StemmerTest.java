package preprocessing;

import junit.framework.TestCase;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.tartarus.snowball.SnowballProgram;
import smile.nlp.stemmer.PorterStemmer;
import smile.nlp.tokenizer.PennTreebankTokenizer;
import weka.core.stemmers.SnowballStemmer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class StemmerTest extends TestCase {
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
    public void testName() throws IOException {
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);

        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);
        Preprocess_doc preprocess_doc = new Preprocess_doc();
        String test = list.get(40);
        String[] parts = test.split("\t");
        int doc_id = Integer.parseInt(parts[0]);
        String doc_corpus = parts[1];
        List<String> pro_doc = new ArrayList<>();
        //in output è la lista delle parole di un documento
        pro_doc = preprocess_doc.preprocess_doc(doc_corpus);
        //PennTreebankTokenizer wordTokenizer = PennTreebankTokenizer.getInstance();
        //String[] inputs = test.split("\t");
        //String s = inputs[1];
        //String[] outputs = wordTokenizer.split(s);
        //List<String> listwords = new ArrayList<>();
        //for(String word : outputs){
        //    listwords.add(word);
        //}
        List<String> stemmed_words = stemming(pro_doc);
        System.out.println(stemmed_words);
    }
}