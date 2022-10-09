package preprocessing;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedReader;
import java.io.File;

public class Stopword_removal {

    public List<String> remove (List<String> list_word) throws IOException {
        String path = "docs/stopwords_eng.txt";
        File file_stopwords = new File("docs/stopwords_eng.txt");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        List<String> list_stopwords = Files.readAllLines(p, StandardCharsets.UTF_8);
        for (int i = 0; i < list_word.size(); i++) {
            for (int j = 0; j < list_stopwords.size(); j++) {
                if(list_word.get(i).equals(list_stopwords.get(j))){
                    list_word.remove(i);
                }
            }
        }
        return list_word;
    }
}
