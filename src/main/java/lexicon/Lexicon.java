package lexicon;

import inverted_index.Posting;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
// contain as key the term_id and the document frequency , the number of document in wich the term appears at least onces
// the key is the pointer to the inverted index
// contain term_id : df
public class Lexicon {

    /**
     * create lexicon from the collection , reading each document
     * term : df (document frequency)
     * @param path of the input file
     * @return
     * @throws IOException
     */
    public Hashtable<String ,Integer> create_lexicon (String path) throws IOException {
        Hashtable<String ,Integer> ht = new Hashtable<>();
        Preprocess_doc preprocess_doc = new Preprocess_doc();
        File file = new File(path);
        List<String> list_doc = new ArrayList<>();
        // to not read all the documents in memory we use a LineIterator
        LineIterator it = FileUtils.lineIterator(file, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                String[] parts = line.split("\t");
                int doc_id = Integer.parseInt(parts[0]);
                String doc_corpus = parts[1];
                List<String> pro_doc = new ArrayList<>();
                //in output è la lista delle parole di un documento
                pro_doc = preprocess_doc.preprocess_doc_optimized(doc_corpus);
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
        } finally {
            LineIterator.closeQuietly(it);
        }
        return ht;
    }


    /**
     * method to save on file the lexicon
     * is written term:df
     * @param map
     */
    public void text_from_lexicon (Hashtable<String ,Integer> map){
        TreeMap<String ,Integer> sortedMap;
        TreeMap<String ,Integer> tmi = new TreeMap<>(map);
        sortedMap = tmi;
        BufferedWriter bf = null;
        String outputFilePath = "docs/lexicon_test.tsv";
        File file = new File(outputFilePath);

        try {

            // create new BufferedWriter for the output file
            bf = new BufferedWriter(new FileWriter(file));

            // iterate map entries
            for (Map.Entry<String, Integer> entry :
                    sortedMap.entrySet()) {

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
     * method to create structure of lexicon in memory from a file
     * we read term::df and is taken term:df
     * @param path file of input
     * @return
     */
    public Hashtable<String ,Integer> lexicon_from_text (String path){
        Hashtable<String ,Integer> map = new Hashtable<>();
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
                String[] parts = line.split(" ");

                // first part is name, second is number
                String name = parts[0].trim();
                String number = parts[1].trim();
                //String number = parts[1];


                // put name, number in HashMap if they are
                // not empty
                if (!name.equals("") && !number.equals(""))
                    map.put(name, Integer.valueOf(number));
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

    /**
     * method to create structure of lexicon in memory from a file
     * we read term:row_id:df and is taken term:df
     * @param path file of input
     * @return
     */
    public Hashtable<String ,Integer> lexicon_from_text_with_freqs (String path){
        Hashtable<String ,Integer> map = new Hashtable<>();
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
                String[] parts = line.split(" ");

                // first part is name, second is number
                String name = parts[0].trim();
                String number = parts[2].trim();
                //String number = parts[1];


                // put name, number in HashMap if they are
                // not empty
                if (!name.equals("") && !number.equals(""))
                    map.put(name, Integer.valueOf(number));
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





