package inverted_index;

import lexicon.Lexicon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Inverted_index {

    HashMap<String, List<Posting>> index = new HashMap();
    public static void createInvertedIndex(String path) throws IOException {
        Preprocess_doc preprocessing = new Preprocess_doc();
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

        for (int i = 0; i < list_doc.size(); i++) {
            String current_doc = list_doc.get(i);
            String[] parts = current_doc.split("\t");
            int doc_id = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> pro_doc = new ArrayList<>();
            pro_doc = preprocessing.preprocess_doc_optimized(doc_corpus);
            long curTime = System.currentTimeMillis();
            final TreeMap<String, ArrayList<String>> dictionary = new TreeMap<String, ArrayList<String>>();
            // Fill the dictionary.
            /*for (final String tokens : pro_doc) {
                if (dictionary.get(parts[0]) == null) {
                    dictionary.put(parts[0], new ArrayList<String>());
                }
                dictionary.get(parts[0]).add(parts[1]);
            }*/

            System.out.println("Sorted in " + ((System.currentTimeMillis() - curTime) / 1000) + " seconds.");
        /*

        Parse the document and read the docs

         */

       /* // Write the block to disk.
        curTime = System.currentTimeMillis();
        final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                 ".txt")));
        for (final Map.Entry<String, ArrayList<String>> entry : dictionary.entrySet()) {
            writer.append(entry.getKey() + "=");
            for (int i = 0; i < entry.getValue().size() - 1; i++)
                writer.append(entry.getValue().get(i) + ",");
            writer.append(entry.getValue().get(entry.getValue().size() - 1));
            writer.append("\n");
        }
        writer.close();
        System.out.println("Block created in " + ((System.currentTimeMillis() - curTime) / 1000) + " seconds.");*/
        }
    }
    public List<Posting> getPostings(String term){
        List<Posting> postingList= new LinkedList<>();
        postingList = index.get(term);
        return postingList;
    }

    public void addPosting(String term, int docid, int freq){
        if(index.get(term) == null){
            List<Posting> l = new LinkedList<>();
            l.add(new Posting(docid, freq));
            index.put(term,l);
        }
        else{
            index.get(term).add(new Posting(docid,freq));
        }
    }

    //TODO: merge method, write to file method, dictionary for each block, get terms, sort posting lists by increasing docid

}

