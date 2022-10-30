package preprocessing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.io.File;

public class StopwordRemoval {

    /**
     * removes the English language stopwords from the token list and returns a new token list.
     * The stopwords are taken from a file
     * @param list_word list of word already tokenized
     * @return list_word list of word without stopwords
     * @throws IOException
     */
    public List<String> remove (List<String> list_word) throws IOException {
        //path of file of stop words
        String path = "docs/stopwords_eng.txt";
        File file_stopwords = new File("docs/stopwords_eng.txt");
        Path p = Paths.get(path);
        List<String> list_stopwords = new ArrayList<>();
        // to not read all the documents in memory we use a LineIterator to improve memory efficiency
        LineIterator it = FileUtils.lineIterator(file_stopwords, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                list_stopwords.add(line);
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        List<String> filtered_words = new ArrayList<>();
        for (String word : list_word) {
            if(!list_stopwords.contains(word)){
                filtered_words.add(word);
            }
        }
        return filtered_words;
    }
}
