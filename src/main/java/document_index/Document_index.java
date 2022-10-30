package document_index;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.PreprocessDoc;

import java.io.*;
import java.util.*;

// contain doc_id : doc.size : pagerank
public class Document_index {
    /**
     * is created the document index with each row :
     * doc_id:document_length
     * @param path
     * @return
     * @throws IOException
     */
    //list is commented becouse in the original slide there is another parameters called PageRank
    public Hashtable<Integer ,/*List<*/Integer/*>*/> create_document_index (String path) throws IOException {
        //Hashtable<Integer ,List<Integer>> ht = new Hashtable<>();
        Hashtable<Integer ,Integer> ht = new Hashtable<>();
        PreprocessDoc preprocess_doc = new PreprocessDoc();
        File file = new File(path);
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
        //List<String> list_doc = Files.readAllLines(p, StandardCharsets.UTF_8);
        int page_rank = 1;
        for (int i = 0; i < list_doc.size(); i++) {
            String current_doc = list_doc.get(i);
            String[] parts = current_doc.split("\t");
            int docid = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> proDoc = new ArrayList<>();
            //in output è la lista delle parole di un documento
            proDoc = preprocess_doc.preprocess_doc_optimized(doc_corpus);
            //ht.put(doc_id, Arrays.asList(pro_doc.size(),page_rank));
            ht.put(docid, proDoc.size());
        }

        return ht;
    }


    /**
     * method to save on a file the  document index structure
     * row : doc_id:doc_length
     * @param map structure to write
     */
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

    /**
     * reconstruction of document index structure from text
     * @param path
     * @return
     */
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

}
