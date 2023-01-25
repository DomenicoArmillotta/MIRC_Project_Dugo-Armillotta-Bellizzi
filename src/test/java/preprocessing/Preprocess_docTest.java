package preprocessing;

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Preprocess_docTest extends TestCase {
    public void testPreMultiple() throws IOException {
        PreprocessDoc preprocess_doc = new PreprocessDoc();
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);


        for(int i=0;i<list.size();i++){
            String test = list.get(i);
            String[] parts = test.split("\t");
            int doc_id = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> pro_doc = new ArrayList<>();
            //in output è la lista delle parole di un documento
            pro_doc = preprocess_doc.preprocess_doc(doc_corpus);
            System.out.println(pro_doc);
        }


    }




    public void testPreOptMultiple() throws IOException {
        PreprocessDoc preprocess_doc = new PreprocessDoc();
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);
        for(int i=0;i<list.size();i++){
            String test = list.get(i);
            String[] parts = test.split("\t");
            int doc_id = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> pro_doc = new ArrayList<>();
            //in output è la lista delle parole di un documento
            pro_doc = preprocess_doc.preprocess_doc(doc_corpus);
            System.out.println(pro_doc);
        }
    }


    public void testPreSingle() throws IOException {
        PreprocessDoc preprocess_doc = new PreprocessDoc();
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);



        String test = list.get(0);
        String[] parts = test.split("\t");
        int doc_id = Integer.parseInt(parts[0]);
        String doc_corpus = parts[1];
        List<String> pro_doc = new ArrayList<>();
        List<String> pro_doc_opt = new ArrayList<>();
        //in output è la lista delle parole di un documento
        pro_doc = preprocess_doc.preprocess_doc(doc_corpus);
        pro_doc_opt = preprocess_doc.preprocess_doc(doc_corpus);
        System.out.println(pro_doc);
        System.out.println(pro_doc_opt);
        assertTrue(pro_doc.equals(pro_doc_opt));


    }

    public void testPreMultipleTogheter() throws IOException {
        PreprocessDoc preprocess_doc = new PreprocessDoc();
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);
        for(int i=0;i<list.size();i++){
            String test = list.get(i);
            String[] parts = test.split("\t");
            int doc_id = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> pro_doc = new ArrayList<>();
            List<String> pro_doc_opt = new ArrayList<>();
            //in output è la lista delle parole di un documento
            pro_doc = preprocess_doc.preprocess_doc(doc_corpus);
            pro_doc_opt = preprocess_doc.preprocess_doc(doc_corpus);
            System.out.println(pro_doc);
            //System.out.println(pro_doc_opt);
            assertTrue(pro_doc.equals(pro_doc_opt));
        }
    }

}