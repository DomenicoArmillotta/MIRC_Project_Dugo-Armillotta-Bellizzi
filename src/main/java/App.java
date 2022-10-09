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
import java.util.List;

/**
 *
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException {
        Normalizer normalizer = new Normalizer();
        Tokenizer tokenizer = new Tokenizer();
        Stopword_removal stopword_removal = new Stopword_removal();
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);
        String test = list.get(40);
        String output_normalizer = normalizer.normalize(test);
        System.out.println(output_normalizer);
        List<String> output_tokenizer = new ArrayList<>();
        output_tokenizer = tokenizer.tokenize(output_normalizer);
        System.out.println(output_tokenizer);
        List<String> output_stopwords_removal = new ArrayList<>();
        output_stopwords_removal = stopword_removal.remove(output_tokenizer);
        System.out.println(output_stopwords_removal);


    }
}
