package document_index;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

// contain doc_id : doc.size : pagerank
public class Document_index {
    public Hashtable<Integer ,/*List<*/Integer/*>*/> create_document_index (String path) throws IOException {
        //Hashtable<Integer ,List<Integer>> ht = new Hashtable<>();
        Hashtable<Integer ,Integer> ht = new Hashtable<>();
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
            //ht.put(doc_id, Arrays.asList(pro_doc.size(),page_rank));
            ht.put(doc_id, pro_doc.size());
        }

        return ht;
    }


    public void text_from_document_index (Hashtable<Integer ,/*List<*/Integer/*>*/> map){
        BufferedWriter bf = null;
        String outputFilePath = "docs/document_index.txt";
        File file = new File(outputFilePath);

        try {

            // create new BufferedWriter for the output file
            bf = new BufferedWriter(new FileWriter(file));

            // iterate map entries
            /*for (Map.Entry<Integer ,List<Integer>> entry :
                    map.entrySet()) {

                // put key and value separated by a colon
                bf.write(entry.getKey() + ":"
                        + entry.getValue().get(0) + ":" + entry.getValue().get(1) );

                // new line
                bf.newLine();
            }*/
            for (Map.Entry<Integer, Integer> entry :
                    map.entrySet()) {

                // put key and value separated by a colon
                bf.write(entry.getKey() + ":"
                        + entry.getValue());

                // new line
                bf.newLine();
            }

            bf.flush();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {

                // always close the writer
                bf.close();
            }
            catch (Exception e) {
            }
        }

    }

    public Hashtable<Integer ,Integer> document_index_from_text (String path){
        Hashtable<Integer , Integer> map = new Hashtable<>();
        //Map<String, Integer> map = new HashMap<String, Integer>();
        BufferedReader br = null;

        try {

            // create file object
            File file = new File(path);

            // create BufferedReader object from the File
            br = new BufferedReader(new FileReader(file));

            String line = null;

            // read file line by line
            while ((line = br.readLine()) != null) {

                // split the line by :
                String[] parts = line.split(":");

                // first part is name, second is number
                String name = parts[0].trim();
                String value1 = parts[1].trim();



                // put name, number in HashMap if they are
                // not empty
                if (!name.equals("") && !value1.equals(""))
                    map.put(Integer.valueOf(name), Integer.valueOf(value1));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {

            // Always close the BufferedReader
            if (br != null) {
                try {
                    br.close();
                }
                catch (Exception e) {
                };
            }
        }

        return map;
    }


    /*public Hashtable<Integer ,List<Integer>> document_index_from_text (String path){
        Hashtable<Integer ,List<Integer>> map = new Hashtable<>();
        //Map<String, Integer> map = new HashMap<String, Integer>();
        BufferedReader br = null;

        try {

            // create file object
            File file = new File(path);

            // create BufferedReader object from the File
            br = new BufferedReader(new FileReader(file));

            String line = null;

            // read file line by line
            while ((line = br.readLine()) != null) {

                // split the line by :
                String[] parts = line.split(":");

                // first part is name, second is number
                String name = parts[0].trim();
                String value1 = parts[1].trim();
                String value2 = parts[2].trim();



                // put name, number in HashMap if they are
                // not empty
                if (!name.equals("") && !value1.equals("") && !value2.equals(""))
                    map.put(Integer.valueOf(name), Arrays.asList(Integer.valueOf(value1),Integer.valueOf(value2)));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {

            // Always close the BufferedReader
            if (br != null) {
                try {
                    br.close();
                }
                catch (Exception e) {
                };
            }
        }

        return map;
    }*/




}
