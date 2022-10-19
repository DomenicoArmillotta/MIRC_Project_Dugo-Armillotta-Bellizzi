package indexing;

import inverted_index.Inverted_index;
import lexicon.Lexicon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import javax.sound.sampled.Line;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class SPIMI_Invert {
    public void spimi_invert_block(String path, int n) throws IOException {
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

    public void spimi_invert_block_dom(String read_path, int n_block) throws IOException {
        File file = new File(read_path);
        LineIterator it = FileUtils.lineIterator(file, "UTF-8");
        long lines = countLineFast(read_path);
        int lines_for_block = (int) Math.ceil(lines / n_block);
        int index_block = 0;
        try {
            while (it.hasNext() && index_block < n_block) {
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
        String[] lex = new String[n];
        String[] tf = new String[n];
        String[] pos = new String[n];
        String[] id = new String[n];

        BufferedReader[] itLex = new BufferedReader[n];
        BufferedReader[] itId = new BufferedReader[n];
        BufferedReader[] itTf = new BufferedReader[n];
        BufferedReader[] itPos = new BufferedReader[n];

        //open all files
        for (int i = 0; i < n; i++) {
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
        int[] cont= new int[n];
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
                HashMap<Integer, Integer> docHt = new HashMap<>();
                HashMap<Integer, String> posHt = new HashMap<>();
                int termf = 0;
                String term = "";
                //HashSet<String> posHt = new HashSet<>();
                for(int i = 0; i < n; i++){
                    String line;// = itLex[i].readLine(); //term of the vocabulary
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
                            String[] docs = docLine.split(" ");
                            int j = 0;
                            for (String doc : docs) {
                                if(doc != " " && doc!= "" && doc!= null) {
                                    //System.out.println(doc);
                                    int docid = Integer.parseInt(doc);
                                    docHt.put(j, docid);
                                    j++;
                                }
                                //System.out.println(docid);
                            }
                            String[] freqs = freqLine.split(" ");
                            for (String f : freqs) {
                                int freq = Integer.parseInt(f);
                                //System.out.println(freq);
                                termf+= freq;
                            }
                            j = 0;
                            String[] positions = posLine.split(" ");
                            for (String p : positions) {
                                if(p != null) {
                                    posHt.put(j, p);
                                    j++;
                                }
                            }
                            /*
                            //open the posting list
                            //System.out.println(term + " = " + lexTerm);
                            String docLine = itId[i].readLine();
                            //System.out.println(docLine);
                            String[] docs = docLine.split(" ");
                            int j = 0;
                            for (String doc : docs) {
                                if(doc != " ") {
                                    //System.out.println(doc);
                                    int docid = Integer.parseInt(doc);
                                    docHt.put(j, docid);
                                    j++;
                                }
                                //System.out.println(docid);
                            }
                            String freqLine = itTf[i].readLine();
                            String[] freqs = freqLine.split(" ");
                            for (String f : freqs) {
                                int freq = Integer.parseInt(f);
                                //System.out.println(freq);
                                termf+= freq;
                            }
                            j = 0;
                            String posLine = itPos[i].readLine();
                            String[] positions = posLine.split(" ");
                            for (String p : positions) {
                                posHt.put(j,p);
                                j++;
                            }*/
                            break;
                            //term = itLex[i].readLine(); //read next term only if it matched the current term
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
                //TODO 19/10/2022: the sorting doesn't always work!
                TreeMap<Integer, Integer> tsdocs = new TreeMap<>(docHt);
                TreeMap<Integer, String> tspos = new TreeMap<>(posHt);
                //TreeSet<String> tspos = new TreeSet<>(posHt);
                if((termf != 0) && (tsdocs != null) && (tspos != null) && (lexTerm!="")) {
                    countTerm++;
                    outDocs.write(String.valueOf(tsdocs.values()));
                    outDocs.newLine(); // new line
                    outFreqs.write(String.valueOf(termf));
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
                }
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
                    /*for (String pathDocIDs : lex) {
                        File fileId = new File(pathDocIDs);
                        LineIterator lineIteratorID = FileUtils.lineIterator(fileId, "UTF-8");
                        while (lineIteratorID.hasNext()) {
                            counterForID++;
                            String w = String.valueOf(lineIteratorID.next());
                            //Integer docID = (Integer) lineIteratorID.next(); //docid of docids file
                            if (w.equals(term)) {
                                //terms.add(String.valueOf(w));
                            } else {
                                continue;
                            }
                        }
                    }*/
        /*
        for (String pathLexicon : lex) {
            File file = new File(pathLexicon);
            LineIterator it = FileUtils.lineIterator(file, "UTF-8");
            while (itTerms.hasNext()) {
                while (it.hasNext()) {
                    String term = it.nextLine(); //term of the dictionary
                    List<String> terms = new LinkedList<String>();
                    terms.add(term);
                    for (String pathDocIDs : lex) {
                        File fileId = new File(pathDocIDs);
                        LineIterator lineIteratorID = FileUtils.lineIterator(fileId, "UTF-8");
                        while (lineIteratorID.hasNext()) {
                            counterForID++;
                            String w = String.valueOf(lineIteratorID.next());
                            //Integer docID = (Integer) lineIteratorID.next(); //docid of docids file
                            if (w.equals(term)) {
                                //terms.add(String.valueOf(w));
                            } else {
                                continue;
                            }
                        }
                    }
                    /*while (terms.iterator().hasNext()) {
                        if (Objects.equals(itTerms.next(), terms.iterator().next())) {
                            // mi segno il la "riga" del match per poi andarla a trovare negli altri file
                            match++;
                            for (String pathDocIDs : id) {
                                File fileId = new File(pathDocIDs);
                                LineIterator lineIteratorID = FileUtils.lineIterator(fileId, "UTF-8");
                                while (lineIteratorID.hasNext()) {
                                    counterForID++;
                                    Integer docID = (Integer) lineIteratorID.next(); //docid of docids file
                                    if (counterForID == match) {
                                        terms.add(String.valueOf(docID));
                                    } else {
                                        continue;
                                    }
                                }
                            }

                            for (String pathForTermFrequency : tf) {
                                File fileTf = new File(pathForTermFrequency);
                                LineIterator lineIteratorTf = FileUtils.lineIterator(fileTf, "UTF-8");
                                while (lineIteratorTf.hasNext()) {
                                    counterForTf++;
                                    Integer termFreq = (Integer) lineIteratorTf.next(); //termFrequency form frequencies file
                                    if (counterForTf == match) {
                                        terms.add(String.valueOf(termFreq));
                                    } else {
                                        continue;
                                    }
                                }
                            }

                            for (String pathForPositions : pos) {
                                File filePos = new File(pathForPositions);
                                LineIterator lineIteratorPos = FileUtils.lineIterator(filePos, "UTF-8");
                                while (lineIteratorPos.hasNext()) {
                                    counterforPs++;
                                    Integer position = (Integer) lineIteratorPos.next(); //position form positions
                                    if (counterforPs == match) {
                                        terms.add(String.valueOf(position));
                                    } else {
                                        continue;
                                    }
                                }
                            }
                        } else {
                            continue;

                        }
                    }
               }


                //here the other lexicons are all open

            }
        }*/
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
