import document_index.Document_index;
import lexicon.Lexicon;
import preprocessing.Normalizer;
import preprocessing.Stopword_removal;
import preprocessing.Tokenizer;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.lang.reflect.InvocationTargetException;
import java.io.Serializable;

/**
 *
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {
        /*
        //declaration element for preprocessing
        Normalizer normalizer = new Normalizer();
        Tokenizer tokenizer = new Tokenizer();
        Stopword_removal stopword_removal = new Stopword_removal();
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);
        String test = list.get(40);
        System.out.println(test);
        String[] parts = test.split("\t");
        System.out.println(parts[0]);

        String output_normalizer = normalizer.normalize(test);
        System.out.println(output_normalizer);
        List<String> output_tokenizer = new ArrayList<>();
        output_tokenizer = tokenizer.tokenize(output_normalizer);
        System.out.println(output_tokenizer);
        List<String> output_stopwords_removal = new ArrayList<>();
        output_stopwords_removal = stopword_removal.remove(output_tokenizer);
        System.out.println(output_stopwords_removal);
        //create lexicon
        Lexicon lexicon = new Lexicon();
        Hashtable<String ,Integer> ht = new Hashtable<>();
        */
        Lexicon lexicon = new Lexicon();
        Document_index document_index = new Document_index();
        String path = "docs/collection_test.tsv";
        //Hashtable<String ,Integer> ht = new Hashtable<>();
        //ht = lexicon.create_lexicon(path);
        //System.out.println(ht);
        Hashtable<Integer ,List<Integer>> ht = new Hashtable<>();
        ht = document_index.create_document_index(path);
        System.out.println(ht);



    }
}
