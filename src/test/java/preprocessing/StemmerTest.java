package preprocessing;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import smile.nlp.stemmer.PorterStemmer;

import java.io.File;
import java.io.IOException;
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
        //BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        List<String> list = new ArrayList<>();
        // to not read all the documents in memory we use a LineIterator
        LineIterator it = FileUtils.lineIterator(file, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                list.add(line);
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        //List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);
        PreprocessDoc preprocess_doc = new PreprocessDoc();
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