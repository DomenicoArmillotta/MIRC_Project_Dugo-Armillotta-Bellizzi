package lexicon;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
// contain as key the term_id and the document frequency , the number of document in wich the term appears at least onces
// the key is the pointer to the inverted index
// contain term_id : df
public class Lexicon {
    public Hashtable<String ,Integer> create_lexicon (String path) throws IOException {
        Hashtable<String ,Integer> ht = new Hashtable<>();
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

        for (int i = 0; i < list_doc.size(); i++) {
            String current_doc = list_doc.get(i);
            String[] parts = current_doc.split("\t");
            int doc_id = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> pro_doc = new ArrayList<>();
            //in output è la lista delle parole di un documento
            pro_doc = preprocess_doc.preprocess_doc(doc_corpus);
            //scorro le parole del documento
            Set<String> mySet = new HashSet<String>();
            for (int j = 0; j < pro_doc.size(); j++) {
                //per evitare i duplicati
                mySet.add(pro_doc.get(j));
            }
            //agiorno hashmap
            for (String key : mySet){
                if(ht.containsKey(key)){
                    ht.put(key, ht.get(key) + 1);
                }else{
                    ht.put(key , 1);
                }
            }


        }


        return ht;
    }


}

