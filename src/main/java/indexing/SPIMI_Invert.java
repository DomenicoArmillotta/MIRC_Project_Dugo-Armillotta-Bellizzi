package indexing;

import document_index.Document_index;
import inverted_index.Inverted_index;
import lexicon.Lexicon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SPIMI_Invert {

    private Hashtable<String, Integer> ht_lexicon = new Hashtable<>();
    private Hashtable<Integer, Integer> ht_docindex = new Hashtable<>();
    /**
     * the collection is divided in n block
     * then we call the function to apply the algorithm to create the inverted index
     * @param read_path input file of entire collection
     * @param n_block number of block that we want
     * @throws IOException
     */
    public void spimi_invert_block(String read_path, int n_block) throws IOException {
        Lexicon lexicon = new Lexicon();
        ht_lexicon = lexicon.create_lexicon(read_path);
        Document_index docindex = new Document_index();
        ht_docindex = docindex.create_document_index(read_path);
        docindex.text_from_document_index(ht_docindex);
        File file = new File(read_path);
        LineIterator it = FileUtils.lineIterator(file, "UTF-8");
        long lines = countLineFast(read_path);
        int lines_for_block = (int) Math.ceil(lines / n_block);
        int index_block = 0;
        try {
            //create chunk of data , splitting in n different block
            while (it.hasNext() && index_block <= n_block) {
                List<String> listDoc = new ArrayList<>();
                int i = 0;
                while (it.hasNext() && i < lines_for_block) {
                    String line = it.nextLine();
                    //System.out.println(line);
                    listDoc.add(line);
                    i++;
                }
                //System.out.println("________Chunk # ------->" + index_block);
                //we elaborate one block at time , so we call the function to create inverted index for the block
                spimi_invert(listDoc, index_block);
                index_block++;
            }

        } finally {
            LineIterator.closeQuietly(it);
        }
        writeAllFilesASCII(n_block); //at the end of the parsing of all the file, merge all the files in the disk
    }

    //we have for each call a block of the file; for each block we create a inverted index with his dictionary and apply the alghorithm;
    //at the end we use the inverted index method to write to the disk

    /**
     * we elaborate one block at time
     * for each block an inverted index is created
     * with the function "writeToDisk" , we wrote on file with the right formatting
     * @param fileBlock
     * @param n
     * @throws IOException
     */
    public void spimi_invert(List<String> fileBlock, int n) throws IOException {
        Inverted_index index = new Inverted_index();//constructor: initializes the dictionary and the output file
        Preprocess_doc preprocessing = new Preprocess_doc();
        for (String doc : fileBlock) { //each row is a doc!
            int cont = 1;
            String[] parts = doc.split("\t");
            int doc_id = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> pro_doc = preprocessing.preprocess_doc_optimized(doc_corpus);
            //read the terms and generate postings
            //write postings
            for (String term : pro_doc) {
                index.addToDict(term);
                index.addPosting(term, doc_id, 1, cont);
                cont++;
            }

        }
        //at the end of the block we have to sort the posting lists in lexicographic order
        index.sortPosting();
        //then we write the block to the disk
        index.writeToDisk(n); //-> created a file for each type of info : doc_id,position,tf,term
    }

    /**
     * this is the merging function of n block created by SPIMI, open all file of the "n" block
     * make a scanner of global lexicon , and check the nextline of each lexicon of n block (lexicon are ordered) ,
     * when a match is found it will merge in the finals file according to the info type (tf,doc_id,position)
     * @param n
     * @throws IOException
     */


    //TODO 22/10/2022: aggiungere la compresssione (da qui o da dentro la creazione dei blocchi?)
    private void writeAllFilesASCII(int n) throws IOException { //writes to the disk all the n block files generated during the algorirhm
        String[] lex = new String[n+1];
        String[] tf = new String[n+1];
        String[] pos = new String[n+1];
        String[] id = new String[n+1];

        BufferedReader[] itLex = new BufferedReader[n+1];
        BufferedReader[] itId = new BufferedReader[n+1];
        BufferedReader[] itTf = new BufferedReader[n+1];
        BufferedReader[] itPos = new BufferedReader[n+1];

        //open all files
        for (int i = 0; i <= n; i++) {
            lex[i] = "docs/lexicon_" + i + ".txt";
            tf[i] = "docs/inverted_index_term_freq_" + i + ".txt";
            pos[i] = "docs/inverted_index_positions_" + i + ".txt";
            id[i] = "docs/inverted_index_docids_" + i + ".txt";
        }

        String outputLex = "docs/lexicon_tot.txt";
        String ouptutDocids = "docs/inverted_index_docids.txt";
        String outputFreqs = "docs/inverted_index_freq.txt";
        String outputPos = "docs/inverted_index_pos.txt";

        //implemento Set --> used for Lookup su Set o(1);
        Set<String> globalTerms = new HashSet<>(ht_lexicon.keySet());
        //Map<String, Integer> globalLexicon = new HashMap<>(ht_lexicon);
        //TreeMap<String, Integer> sortedLex = new TreeMap<>(globalLexicon);
        TreeSet<String> sortedTerms = new TreeSet<>(globalTerms);
        LinkedHashSet<String> termSet = new LinkedHashSet<>(sortedTerms);
        //Iterator<String> itTerms = sortedTerms.iterator(); //--> iterator for all term in collection
        Iterator<String> itTerms = termSet.iterator(); //--> iterator for all term in collection
        //LinkedList<String> lengthPosting = new LinkedList<String>(sortedTerms);
        //Iterator<String> itTerms = lengthPosting.iterator();

        BufferedWriter outLex = null;
        BufferedWriter outDocs = null;
        BufferedWriter outFreqs = null;
        BufferedWriter outPos = null;

        try {
            outLex = new BufferedWriter(new FileWriter(new File(outputLex)));
            outDocs = new BufferedWriter(new FileWriter(new File(ouptutDocids)));
            outFreqs = new BufferedWriter(new FileWriter(new File(outputFreqs)));
            outPos = new BufferedWriter(new FileWriter(new File(outputPos)));
            int countTerm = 0;

            //iterate through all term of collections
            while (itTerms.hasNext()) {
            //for(String lexTerm: termSet){
                String lexTerm = itTerms.next();
                Map<Integer,String> posMap = new HashMap<>(); //--> contains position for each doc_id
                Map<Integer,Integer> freqMap = new HashMap<>(); //--> contains term freq for each doc_id
                HashSet<Integer> docHs = new HashSet<>(); //--> contains doc_id
                String term = "";
                //iterate through all block
                for(int i = 0; i <= n; i++){
                    int j = 0;
                    String line; //term of the vocabulary
                    itLex[i] = Files.newBufferedReader(Paths.get(lex[i]), StandardCharsets.UTF_8);
                    itId[i] = Files.newBufferedReader(Paths.get(id[i]), StandardCharsets.UTF_8);
                    itTf[i] = Files.newBufferedReader(Paths.get(tf[i]), StandardCharsets.UTF_8);
                    itPos[i] = Files.newBufferedReader(Paths.get(pos[i]), StandardCharsets.UTF_8);
                    //iterate through all lexicon of all block
                    while ((line = itLex[i].readLine()) != null) {
                        //List<String> terms = new LinkedList<String>();
                        //splitted for the offset
                        String[] inputs = line.split(" ");
                        term = inputs[0];
                        int offset = Integer.parseInt(inputs[1]);
                        //if a match is founded  , a merge is made
                        //to reach the right line on files , an offset is used
                        if (lexTerm.equals(term)) {
                            int countLine = 0;
                            String docLine = itId[i].readLine(); //--> doc_id
                            String freqLine = itTf[i].readLine(); //--> term freq.
                            String posLine = itPos[i].readLine(); //--> String of positions
                            while(countLine != offset){
                                docLine = itId[i].readLine();
                                freqLine = itTf[i].readLine();
                                posLine = itPos[i].readLine();
                                countLine++;
                            }
                            //now we take the docids and positions of the term
                            //we take the docids and then map the positions to them, so they are ordered in the same way
                            //we do the same for term frequencies: we map to docs and sum for the same docs
                            String[] docs = docLine.split(" ");
                            String[] positions = posLine.split(" ");
                            String[] freqs = freqLine.split(" ");
                            //iteration of all doc_id for the term
                            //save on data structure doc_id , term freq. , position to merge with all doc and then write
                            for (String doc : docs) {
                                    //System.out.println(doc);
                                    int docid = Integer.parseInt(doc);
                                    //docHt.put(j, docid);
                                    docHs.add(docid);
                                    posMap.put(docid, positions[j]);
                                    int freq = Integer.parseInt(freqs[j]);
                                    freqMap.put(docid, freq);
                                    j++;
                            }
                            break;
                        }

                    }
                }
                TreeSet<Integer> tsdocs = new TreeSet<>(docHs);
                TreeMap<Integer, String> tspos = new TreeMap<>(posMap);
                TreeMap<Integer, Integer> tsfreq = new TreeMap<>(freqMap);
                countTerm++;
                int lengthPostingList = tsdocs.size();
                //write on different doc different type of value
                // new line for each doc for : doc_id , tfreq. , pos
                outDocs.write(String.valueOf(tsdocs));
                outDocs.newLine(); // new line on doc file
                outFreqs.write(String.valueOf(tsfreq.values()));
                outFreqs.newLine(); // new line on freq file
                outPos.write(String.valueOf(tspos.values()));
                outPos.newLine(); // new line on pos file
                //int docfreq = sortedLex.get(lexTerm);
                lexTerm += " " + countTerm + " " + lengthPostingList;// + " " + docfreq;
                outLex.write(lexTerm);
                outLex.newLine();
                outLex.flush();
                outDocs.flush();
                outFreqs.flush();
                outPos.flush();
            }

        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {
                // always close the writer at the end of merging phase
                outDocs.close();
                outFreqs.close();
                outPos.close();
                outLex.close();
            }
            catch (Exception e) {
            }
        }
    }


    private void writeAllFilesBin(int n) throws IOException { //writes to the disk all the n block files generated during the algorirhm
        //TODO 22/10/2022: implement the binary version of the index merging
        String[] lex = new String[n+1];
        String[] tf = new String[n+1];
        String[] pos = new String[n+1];
        String[] id = new String[n+1];

        BufferedReader[] itLex = new BufferedReader[n+1];
        BufferedReader[] itId = new BufferedReader[n+1];
        BufferedReader[] itTf = new BufferedReader[n+1];
        BufferedReader[] itPos = new BufferedReader[n+1];

        //open all files
        for (int i = 0; i <= n; i++) {
            lex[i] = "docs/lexicon_" + i + ".txt";
            tf[i] = "docs/inverted_index_term_freq_" + i + ".txt";
            pos[i] = "docs/inverted_index_positions_" + i + ".txt";
            id[i] = "docs/inverted_index_docids_" + i + ".txt";
        }

        String outputLex = "docs/lexicon_tot.bin";
        String ouptutDocids = "docs/inverted_index_docids.bin";
        String outputFreqs = "docs/inverted_index_freq.bin";
        String outputPos = "docs/inverted_index_pos.bin";
        String input_docs = "docs/collection_test.tsv";

        Lexicon lexicon = new Lexicon();
        Hashtable<String, Integer> ht_lexicon = new Hashtable<>();
        ht_lexicon = lexicon.create_lexicon(input_docs);
        //implemento Set --> used for Lookup su Set o(1);
        Set<String> globalTerms = new HashSet<>(ht_lexicon.keySet());
        TreeSet<String> sortedTerms = new TreeSet<>(globalTerms);
        Iterator<String> itTerms = sortedTerms.iterator(); //--> iterator for all term in collection
        LinkedList<String> lengthPosting = new LinkedList<String>(globalTerms);
        Iterator<String> itPostings = lengthPosting.iterator();

        BufferedOutputStream outLex = null;
        BufferedOutputStream outDocs = null;
        BufferedOutputStream outFreqs = null;
        BufferedOutputStream outPos = null;

        try {
            outLex = new BufferedOutputStream(new FileOutputStream(new File(outputLex)));
            outDocs = new BufferedOutputStream(new FileOutputStream(new File(ouptutDocids)));
            outFreqs = new BufferedOutputStream(new FileOutputStream(new File(outputFreqs)));
            outPos = new BufferedOutputStream(new FileOutputStream(new File(outputPos)));
            //String contentLexicon = "------------------------------LEXICON--------------------------\n"+"Term"+"--"+"Occurences"+"--"+"Posting List size\n";
            //outLex.write(contentLexicon);
            int countTerm = 0;

            //iterate through all term of collections
            while (itTerms.hasNext()) {
                String lexTerm = itTerms.next();
                Map<Integer,String> posMap = new HashMap<>(); //--> contains position for each doc_id
                Map<Integer,Integer> freqMap = new HashMap<>(); //--> contains term freq for each doc_id
                HashSet<Integer> docHs = new HashSet<>(); //--> contains doc_id
                int termf = 0;
                String term = "";
                //iterate through all block
                for(int i = 0; i <= n; i++){
                    int j = 0;
                    String line; //term of the vocabulary
                    itLex[i] = Files.newBufferedReader(Paths.get(lex[i]), StandardCharsets.UTF_8);
                    itId[i] = Files.newBufferedReader(Paths.get(id[i]), StandardCharsets.UTF_8);
                    itTf[i] = Files.newBufferedReader(Paths.get(tf[i]), StandardCharsets.UTF_8);
                    itPos[i] = Files.newBufferedReader(Paths.get(pos[i]), StandardCharsets.UTF_8);
                    //iterate through all lexicon of all block
                    while ((line = itLex[i].readLine()) != null) {
                        //System.out.println(i + " " + term + " " + lexTerm);
                        List<String> terms = new LinkedList<String>();
                        //splitted for the offset
                        String[] inputs = line.split(" ");
                        term = inputs[0];
                        int offset = Integer.parseInt(inputs[1]);
                        //if a match is founded  , a merge is made
                        //to reach the right line on files , an offset is used
                        if (lexTerm.equals(term)) {
                            int countLine = 0;
                            String docLine = itId[i].readLine(); //--> doc_id
                            String freqLine = itTf[i].readLine(); //--> term freq.
                            String posLine = itPos[i].readLine(); //--> String of positions
                            while(countLine != offset){
                                docLine = itId[i].readLine();
                                freqLine = itTf[i].readLine();
                                posLine = itPos[i].readLine();
                                countLine++;
                            }
                            //now we take the docids and positions of the term
                            //we take the docids and then map the positions to them, so they are ordered in the same way
                            //we do the same for term frequencies: we map to docs and sum for the same docs
                            String[] docs = docLine.split(" ");
                            String[] positions = posLine.split(" ");
                            String[] freqs = freqLine.split(" ");
                            //iteration of all doc_id for the term
                            //save on data structure doc_id , term freq. , position to merge with all doc and then write
                            for (String doc : docs) {
                                //System.out.println(doc);
                                int docid = Integer.parseInt(doc);
                                //docHt.put(j, docid);
                                docHs.add(docid);
                                posMap.put(docid, positions[j]);
                                int freq = Integer.parseInt(freqs[j]);
                                freqMap.put(docid, freq);
                                j++;
                                //System.out.println(freq);
                                    /*if(freqMap.get(j) == null){
                                        freqMap.put(j, freq);
                                    }
                                    else {
                                        termf = freqMap.get(j);
                                        termf += freq;
                                        freqMap.put(j, termf);
                                    }*/

                                //System.out.println(docid);
                            }
                            break;
                        }

                    }
                }

                //TreeSet<Integer> tsdocs = new TreeSet<>(docHt.values());
                TreeSet<Integer> tsdocs = new TreeSet<>(docHs);
                TreeMap<Integer, String> tspos = new TreeMap<>(posMap);
                TreeMap<Integer, Integer> tsfreq = new TreeMap<>(freqMap);
                countTerm++;
                int lengthPostingList = tsdocs.size();
                //write on different doc different type of value
                // new line for each doc for : doc_id , tfreq. , pos
                //TODO 22/10/2022: convertire i dati in binario (compressi!!!) e poi scriverli su file
                //outDocs.write(String.valueOf(tsdocs));
                outDocs.write('\n'); // new line on doc file
                //outFreqs.write(String.valueOf(tsfreq.values()));
                outDocs.write('\n'); // new line on freq file
                //outPos.write(String.valueOf(tspos.values()));
                outDocs.write('\n'); // new line on pos file
                lexTerm += " " + countTerm + " " + lengthPostingList;
                //outLex.write(lexTerm);
                outDocs.write('\n');
                outLex.flush();
                outDocs.flush();
                outFreqs.flush();
                outPos.flush();
            }

        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {
                // always close the writer at the end of merging phase
                outDocs.close();
                outFreqs.close();
                outPos.close();
                outLex.close();
            }
            catch (Exception e) {
            }
        }
    }

    //fast to count file line --> 5milion in 4/5 s

    /**
     * count the number of lines in the file
     * it should be the fastest way possible according to online benchmark
     * @param fileName_path
     * @return
     */
    public static long countLineFast(String fileName_path) {

        long lines = 0;

        try (InputStream is = new BufferedInputStream(new FileInputStream(fileName_path))) {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean endsWithoutNewLine = false;
            while ((readChars = is.read(c)) != -1) {
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n')
                        ++count;
                }
                endsWithoutNewLine = (c[readChars - 1] != '\n');
            }
            if (endsWithoutNewLine) {
                ++count;
            }
            lines = count;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lines;
    }


}
