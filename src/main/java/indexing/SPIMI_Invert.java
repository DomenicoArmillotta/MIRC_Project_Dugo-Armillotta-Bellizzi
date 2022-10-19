package indexing;

import inverted_index.Inverted_index;
import lexicon.Lexicon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SPIMI_Invert {
    public void spimi_invert_block_pre(String path, int n) throws IOException {
        //make pre processing of text
        Preprocess_doc preprocessing = new Preprocess_doc();
        File file = new File(path);
        Path p = Paths.get(path);
        List<String> list_doc = new ArrayList<>();
        long lines = getFileLines(path);
        //load chunk of file of fixed size --> to follow the algorithm
        int CHUNKSIZE = (int) Math.ceil(lines / n); //size of each block of lines
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        String textfileRow = null;
        List<String> fileLines = new ArrayList<>();
        int lineIndex = 0;
        int nChunk = 0;
        while (lineIndex < lines) //for each chunk we read all the lines and apply SPIMI
        {
            fileLines.add(textfileRow);
            int chunkEnd = lineIndex + CHUNKSIZE;

            if (chunkEnd >= fileLines.size()) {
                chunkEnd = fileLines.size();
            }
            //for each block of files, I call SPIMI
            List<String> mySubList = fileLines.subList(lineIndex, chunkEnd);
            spimi_invert(mySubList, nChunk);
            nChunk++;
            lineIndex = chunkEnd;
        }
        //merge results of each block into one file: for each chunk (0 to nChunk-1) with
        writeAllFiles(n);
    }

    public void spimi_invert_block(String read_path, int n_block) throws IOException {
        File file = new File(read_path);
        LineIterator it = FileUtils.lineIterator(file, "UTF-8");
        long lines = countLineFast(read_path);
        int lines_for_block = (int) Math.ceil(lines / n_block);
        int index_block = 0;
        try {
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
                spimi_invert(listDoc, index_block);
                index_block++;
            }

        } finally {
            LineIterator.closeQuietly(it);
        }
        writeAllFiles(n_block); //at the end of the parsing of all the file, merge all the files in the disk
    }

    //we have for each call a block of the file; for each block we create a inverted index with his dictionary and apply the alghorithm;
    //at the end we use the inverted index method to write to the disk
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
        index.writeToDisk(n);
    }

    private void writeAllFiles(int n) throws IOException { //writes to the disk all the n block files generated during the algorirhm
        //TODO 13/10/2022: implement the index merging to merge dictionary files and inverted index files in the disk
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
        String input_docs = "docs/collection_test.tsv";

        Lexicon lexicon = new Lexicon();
        Hashtable<String, Integer> ht_lexicon = new Hashtable<>();
        ht_lexicon = lexicon.create_lexicon(input_docs);
        //implemento Set --> Lookup su Set o(1);
        Set<String> globalTerms = new HashSet<>(ht_lexicon.keySet());
        TreeSet<String> sortedTerms = new TreeSet<>(globalTerms);
        Iterator<String> itTerms = sortedTerms.iterator();

        int match = 0;
        int[] cont= new int[n+1];
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
            while (itTerms.hasNext()) {
                String lexTerm = itTerms.next();
                HashMap<Integer, Integer>docHt = new HashMap<>();
                //HashMap<Integer, String> posHt = new HashMap<>();
                Map<Integer,String> posMap = new HashMap<>();
                Map<Integer,Integer> freqMap = new HashMap<>();
                int termf = 0;
                String term = "";
                //HashSet<String> posHt = new HashSet<>();
                for(int i = 0; i <= n; i++){
                    String line; //term of the vocabulary
                    /*List<String> docIdTot = new LinkedList<String>();
                    List<String> tfTot = new LinkedList<String>();
                    List<String> posTot = new LinkedList<String>();*/
                    itLex[i] = Files.newBufferedReader(Paths.get(lex[i]), StandardCharsets.UTF_8);
                    itId[i] = Files.newBufferedReader(Paths.get(id[i]), StandardCharsets.UTF_8);
                    itTf[i] = Files.newBufferedReader(Paths.get(tf[i]), StandardCharsets.UTF_8);
                    itPos[i] = Files.newBufferedReader(Paths.get(pos[i]), StandardCharsets.UTF_8);
                    while ((line = itLex[i].readLine()) != null) {
                        //System.out.println(i + " " + term + " " + lexTerm);
                        List<String> terms = new LinkedList<String>();
                        //terms.add(term);
                        String[] inputs = line.split(" ");
                        term = inputs[0];
                        int offset = Integer.parseInt(inputs[1]);
                        if (lexTerm.equals(term)) {
                            int countLine = 0;
                            String docLine = itId[i].readLine();
                            String freqLine = itTf[i].readLine();
                            String posLine = itPos[i].readLine();
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
                            int j = 0;
                            for (String doc : docs) {
                                if(doc != " " && doc!= "" && doc!= null) {
                                    //System.out.println(doc);
                                    int docid = Integer.parseInt(doc);
                                    docHt.put(j, docid);
                                    posMap.put(docid, positions[j]);
                                    int freq = Integer.parseInt(freqs[j]);
                                    //System.out.println(freq);
                                    if(freqMap.get(docid) == null){
                                        freqMap.put(docid, freq);
                                    }
                                    else {
                                        termf = freqMap.get(docid);
                                        termf += freq;
                                        freqMap.put(docid, termf);
                                    }
                                    j++;
                                }
                                //System.out.println(docid);
                            }
                            break;
                        }
                        /*for(BufferedReader br: itLex){
                            br.close();
                        }
                        for(BufferedReader br: itId){
                            br.close();
                        }
                        for(BufferedReader br: itPos){
                            br.close();
                        }
                        for(BufferedReader br: itTf){
                            br.close();
                        }*/
                    }
                }
                TreeSet<Integer> tsdocs = new TreeSet<>(docHt.values());
                TreeMap<Integer, String> tspos = new TreeMap<>(posMap);
                TreeMap<Integer, Integer> tsfreq = new TreeMap<>(freqMap);
                //TreeSet<String> tspos = new TreeSet<>(posHt);
                //if((tsfreq != null) && (tsdocs != null) && (tspos != null) && (lexTerm!="")) {
                countTerm++;
                outDocs.write(String.valueOf(tsdocs));
                outDocs.newLine(); // new line
                outFreqs.write(String.valueOf(tsfreq.values()));
                outFreqs.newLine(); // new line
                outPos.write(String.valueOf(tspos.values()));
                outPos.newLine(); // new line
                //TODO 19/10/2022: add also the other parameters to the lexicon! (e.g. posting list length...)
                lexTerm += " " + countTerm;
                outLex.write(lexTerm);
                outLex.newLine();
                outLex.flush();
                outDocs.flush();
                outFreqs.flush();
                outPos.flush();
                //}
            }

        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {
                // always close the writer
                outDocs.close();
                outFreqs.close();
                outPos.close();
                outLex.close();
            }
            catch (Exception e) {
            }
        }
    }

    private static long getFileLines(String path) {
        long result = 0;
        try {
            result = Files.lines(Paths.get(path)).count();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    //fast to count file line --> 5milion in 4/5 s
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
