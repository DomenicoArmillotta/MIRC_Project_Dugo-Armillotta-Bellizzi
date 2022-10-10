package document_index;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
// contain doc_id : doc.size : pagerank
public class Document_index {
    public Hashtable<Integer ,List<Integer>> create_document_index (String path) throws IOException {
        Hashtable<Integer ,List<Integer>> ht = new Hashtable<>();
        Preprocess_doc preprocess_doc = new Preprocess_doc();
        File file = new File(path);
        Path p = Paths.get(path);
        List<String> list_doc = new ArrayList<>();
        // to not read all the documents in memory we use a LineIterator
        LineIterator it = FileUtils.lineIterator(file, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                list_doc.add(line);
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        //BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        //PROBLEMA = voglio leggere 100 righe alla volta
        //List<String> list_doc = Files.readAllLines(p, StandardCharsets.UTF_8);
        int page_rank = 1;
        for (int i = 0; i < list_doc.size(); i++) {
            String current_doc = list_doc.get(i);
            String[] parts = current_doc.split("\t");
            int doc_id = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> pro_doc = new ArrayList<>();
            //in output è la lista delle parole di un documento
            pro_doc = preprocess_doc.preprocess_doc_optimized(doc_corpus);
            ht.put(doc_id, Arrays.asList(pro_doc.size(),page_rank));
        }

        return ht;
    }
}
