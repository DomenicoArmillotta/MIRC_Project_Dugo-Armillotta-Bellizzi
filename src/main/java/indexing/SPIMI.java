package indexing;

import document_index.DocumentIndex;
import inverted_index.Compressor;
import inverted_index.InvertedIndex;
import inverted_index.Posting;
import lexicon.Lexicon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import preprocessing.PreprocessDoc;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SPIMI {

    private Hashtable<String, Integer> ht_lexicon = new Hashtable<>();
    private Hashtable<Integer, Integer> ht_docindex = new Hashtable<>();
    private DB db;
    private HTreeMap<String, Integer> documentIndex;

    public int doc_id = 0;
    /**
     * the collection is divided in n block
     * then we call the function to apply the algorithm to create the inverted index
     * @param read_path input file of entire collection
     * @param n_block number of block that we want
     * @throws IOException
     */
    public void spimiInvertBlock(String read_path, int n_block) throws IOException {
        Lexicon lexicon = new Lexicon();
        ht_lexicon = lexicon.createLexicon(read_path);
        DocumentIndex docindex = new DocumentIndex();
        ht_docindex = docindex.createDocumentIndex(read_path);
        docindex.textFromDocumentIndex(ht_docindex);
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
                    listDoc.add(line);
                    i++;
                }
                //we elaborate one block at time , so we call the function to create inverted index for the block
                spimiInvert(listDoc, index_block);
                index_block++;
            }

        } finally {
            LineIterator.closeQuietly(it);
        }
        writeAllFilesASCII(n_block-1); //at the end of the parsing of all the file, merge all the files in the disk
    }

    public void spimiInvertBlockCompression(String read_path, int n_block) throws IOException {
        Lexicon lexicon = new Lexicon();
        ht_lexicon = lexicon.createLexicon(read_path);
        DocumentIndex docindex = new DocumentIndex();
        ht_docindex = docindex.createDocumentIndex(read_path);
        docindex.textFromDocumentIndex(ht_docindex);
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
                    listDoc.add(line);
                    i++;
                }
                //we elaborate one block at time , so we call the function to create inverted index for the block
                spimiInvert(listDoc, index_block);
                index_block++;
            }

        } finally {
            LineIterator.closeQuietly(it);
        }
        writeAllFilesBin(n_block-1); //at the end of the parsing of all the file, merge all the files in the disk and write a bin file
    }

    public void spimiInvertBlockWithRamUsage(String read_path) throws IOException {
        int n_block = 0;
        Lexicon lexicon = new Lexicon();
        ht_lexicon = lexicon.createLexicon(read_path);
        DocumentIndex docindex = new DocumentIndex();
        ht_docindex = docindex.createDocumentIndex(read_path);
        docindex.textFromDocumentIndex(ht_docindex);
        File input_file = new File(read_path);
        LineIterator it = FileUtils.lineIterator(input_file, "UTF-8");
        int index_block = 0;
        try {
            //create chunk of data , splitting in n different block
            while (it.hasNext() && (Runtime.getRuntime().totalMemory()*0.80 <= Runtime.getRuntime().freeMemory())){  //--> its the ram of jvm
                List<String> listDoc = new ArrayList<>();
                int i = 0;
                while (it.hasNext()) {
                    String line = it.nextLine();
                    listDoc.add(line);
                    i++;
                }
                //we elaborate one block at time , so we call the function to create inverted index for the block
                spimiInvert(listDoc, index_block);
                n_block++;
                index_block++;
            }

        } finally {
            LineIterator.closeQuietly(it);
        }
        writeAllFilesASCII(n_block-1); //at the end of the parsing of all the file, merge all the files in the disk
    }
    public void spimiInvertBlockWithRamUsageCompressed(String read_path) throws IOException {
        int n_block = 0;
        Lexicon lexicon = new Lexicon();
        ht_lexicon = lexicon.createLexicon(read_path);
        DocumentIndex docindex = new DocumentIndex();
        ht_docindex = docindex.createDocumentIndex(read_path);
        docindex.textFromDocumentIndex(ht_docindex);
        File input_file = new File(read_path);
        LineIterator it = FileUtils.lineIterator(input_file, "UTF-8");
        int index_block = 0;
        try {
            //create chunk of data , splitting in n different block
            while (it.hasNext() && (Runtime.getRuntime().totalMemory()*0.80 <= Runtime.getRuntime().freeMemory())) {  //--> its the ram of jvm
                List<String> listDoc = new ArrayList<>();
                int i = 0;
                while (it.hasNext()) {
                    String line = it.nextLine();
                    listDoc.add(line);
                    i++;
                }
                //we elaborate one block at time , so we call the function to create inverted index for the block
                spimiInvert(listDoc, index_block);
                n_block++;
                index_block++;
            }

        } finally {
            LineIterator.closeQuietly(it);
        }
        writeAllFilesBin(n_block-1); //at the end of the parsing of all the file, merge all the files in the disk
    }

    public void spimiInvertBlockMapped(String read_path) throws IOException {
        int n_block = 0;
        db = DBMaker.fileDB("docs/testDB.db").make();
        documentIndex = db
                .hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
        File input_file = new File(read_path);
        LineIterator it = FileUtils.lineIterator(input_file, "UTF-8");
        int index_block = 0;
        try {
            //create chunk of data , splitting in n different block
            while (it.hasNext() && (Runtime.getRuntime().totalMemory()*0.80 <= Runtime.getRuntime().freeMemory())){  //--> its the ram of jvm
                List<String> listDoc = new ArrayList<>();
                int i = 0;
                while (it.hasNext()) {
                    String line = it.nextLine();
                    listDoc.add(line);
                    i++;
                }
                //we elaborate one block at time , so we call the function to create inverted index for the block
                spimiInvertMapped(listDoc, index_block);
                n_block++;
                index_block++;
            }

        } finally {
            LineIterator.closeQuietly(it);
        }
        //writeAllFilesASCII(n_block-1); //at the end of the parsing of all the file, merge all the files in the disk
    }


    /**
     * we elaborate one block at time
     * for each block an inverted index is created
     * with the function "writeToDisk" , we wrote on file with the right formatting
     * @param fileBlock
     * @param n
     * @throws IOException
     */
    public void spimiInvert(List<String> fileBlock, int n) throws IOException {
        InvertedIndex index = new InvertedIndex();//constructor: initializes the dictionary and the output file
        PreprocessDoc preprocessing = new PreprocessDoc();
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

    public void spimiInvertMapped(List<String> fileBlock, int n) throws IOException {
        InvertedIndex index = new InvertedIndex(n);//constructor: initializes the dictionary and the output file
        PreprocessDoc preprocessDoc = new PreprocessDoc();
        for (String doc : fileBlock) { //each row is a doc!
            int cont = 1;
            String[] parts = doc.split("\t");
            String docno = parts[0];
            String doc_corpus = parts[1];
            List<String> pro_doc = preprocessDoc.preprocess_doc_optimized(doc_corpus);
            //read the terms and generate postings
            //write postings
            for (String term : pro_doc) {
                index.addToLexicon(term);
                index.addPosting(term, doc_id, 1);
                cont++;
            }
            documentIndex.put(docno, cont);
            doc_id++;

        }
        //at the end of the block we have to sort the posting lists in lexicographic order
        //index.sortPosting();
        //then we write the block to the disk
        //index.writeToDisk(n); //-> created a file for each type of info : doc_id,position,tf,term
    }

    private void mergeBlocks(int n){
        String[] lex = new String[n+1];
        String[] tf = new String[n+1];
        String[] id = new String[n+1];

        //open all files
        for (int i = 0; i <= n; i++) {
            lex[i] = "docs/lexicon_" + i;
            tf[i] = "docs/inverted_index_term_freq_" + i;
            id[i] = "docs/inverted_index_docids_" + i;
        }
    }

    /**
     * this is the merging function of n block created by SPIMI, open all file of the "n" block
     * make a scanner of global lexicon , and check the nextline of each lexicon of n block (lexicon are ordered) ,
     * when a match is found it will merge in the finals file according to the info type (tf,doc_id,position)
     * we save on a .text file using ASCII formatting
     * @param n
     * @throws IOException
     */

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
        TreeSet<String> sortedTerms = new TreeSet<>(globalTerms);
        //LinkedHashSet: the fastest way to iterate over a hashset
        LinkedHashSet<String> termSet = new LinkedHashSet<>(sortedTerms);
        Iterator<String> itTerms = termSet.iterator(); //--> iterator for all term in collection

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
                int npostings = 0; //to count the posting list size
                //iterate through all block
                for(int i = 0; i <= n; i++){
                    //int j = 0;
                    String line = ""; //term of the vocabulary
                    itLex[i] = Files.newBufferedReader(Paths.get(lex[i]), StandardCharsets.UTF_8);
                    itId[i] = Files.newBufferedReader(Paths.get(id[i]), StandardCharsets.UTF_8);
                    itTf[i] = Files.newBufferedReader(Paths.get(tf[i]), StandardCharsets.UTF_8);
                    itPos[i] = Files.newBufferedReader(Paths.get(pos[i]), StandardCharsets.UTF_8);
                    //iterate through all lexicon of all block
                    while ((line = itLex[i].readLine()) != null) {
                        //splitted for the offset
                        //1,000,000 iterations of split take 3.36s,
                        //while 1,000,000 iterations of substring take only 0.05s.
                        term = line.substring(0, line.indexOf(" "));
                        String input = line.substring(line.indexOf(" ")+1, line.length());
                        int offset = Integer.parseInt(input);
                        //if a match is founded  , a merge is made
                        //to reach the right line on files , an offset is used
                        if (lexTerm.equals(term)) {
                            int countLine = 0;
                            //read the postings at the desired offset
                            String docLine = (String) FileUtils.readLines(new File(id[i]), "UTF-8").get(offset); //--> doc_id of selected term
                            String freqLine = (String) FileUtils.readLines(new File(tf[i]), "UTF-8").get(offset); //--> term freq of selected term
                            String posLine = (String) FileUtils.readLines(new File(pos[i]), "UTF-8").get(offset); //--> String of positions of selected term
                            //now we take the docids and positions of the term
                            //we take the docids and then map the positions to them, so they are ordered in the same way
                            //we do the same for term frequencies: we map to docs and sum for the same docs
                            int countDoc = docLine.indexOf(" "); // in our text the doc_id are separated by white space (" ")
                            int countFreq = freqLine.indexOf(" ");
                            int countPos = posLine.indexOf(" ");
                            String docs = "";
                            String freqs = "";
                            String poss = "";
                            //we read the postings with substring because it's faster
                            if(countDoc == -1){ //if is not founded " " , mean that there is only one value , so a parsing is computed
                                int docid = Integer.parseInt(docLine);
                                docs+=docid;
                                docHs.add(docid);
                                posMap.put(docid, posLine); //reads the list of positions
                                int freq = Integer.parseInt(freqLine);
                                poss+=posLine;
                                freqs+=freq;
                                freqMap.put(docid, freq);
                                outDocs.write(docs + " ");
                                outFreqs.write(freqs + " ");
                                outPos.write(poss + " ");
                                npostings++;
                                break;
                            }
                            //iterate thought all doc_id of selected term
                            while(docLine!= ""){
                                int docid = Integer.parseInt(docLine.substring(0, countDoc));
                                docHs.add(docid);
                                posMap.put(docid, posLine.substring(0, countPos));
                                int freq = Integer.parseInt(freqLine.substring(0,countFreq));
                                freqMap.put(docid, freq);
                                docHs.add(docid);
                                String newPos = posLine.substring(0, countPos);
                                posMap.put(docid, posLine.substring(0, countPos));
                                String nextDoc = docLine.substring(docLine.indexOf(" ")+1); //--> next doc_id
                                String nextFreq = freqLine.substring(freqLine.indexOf(" ")+1); //--> next freq
                                String nextPos = posLine.substring(posLine.indexOf(" ")+1); //--> next position set
                                //if there are other values, then the next posting is not the last
                                // and so take the next posting separated by space, otherwise take the last posting
                                countDoc = nextDoc.indexOf(" ") == -1 ? nextDoc.length()-1 : nextDoc.indexOf(" ");
                                countFreq = nextFreq.indexOf(" ") == -1 ? nextFreq.length()-1 : nextFreq.indexOf(" ");
                                countPos = nextPos.indexOf(" ") == -1 ? nextPos.length()-1 : nextPos.indexOf(" ");
                                docs = docid + " ";
                                poss = newPos + " ";
                                freqs = freq + " ";
                                outDocs.write(docs);
                                outFreqs.write(freqs);
                                outPos.write(poss);
                                docLine = nextDoc;
                                freqLine = nextFreq;
                                posLine = nextPos;
                                npostings++;
                            }
                            break;

                        }

                    }
                }
                countTerm++; //increment the offset
                outDocs.newLine(); // new line on doc file
                outFreqs.newLine(); // new line on freq file
                outPos.newLine(); // new line on pos file
                lexTerm += " " + countTerm + " " + npostings;// + " " + docfreq;
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




    /**
     * this is the merging function of n block created by SPIMI, open all file of the "n" block
     * make a scanner of global lexicon , and check the nextline of each lexicon of n block (lexicon are ordered) ,
     * when a match is found it will merge in the finals file according to the info type (tf,doc_id,position)
     * we save on a .bin file using Binary formatting
     * @param n
     * @throws IOException
     */
    //TODO 22/10/2022: aggiungere la compresssione (da qui o da dentro la creazione dei blocchi?)
    private void writeAllFilesBin(int n) throws IOException { //writes to the disk all the n block files generated during the algorirhm
        String[] lex = new String[n+1];
        String[] tf = new String[n+1];
        String[] id = new String[n+1];

        BufferedReader[] itLex = new BufferedReader[n+1];
        BufferedReader[] itId = new BufferedReader[n+1];
        BufferedReader[] itTf = new BufferedReader[n+1];

        //open all files
        for (int i = 0; i <= n; i++) {
            lex[i] = "docs/lexicon_" + i + ".txt";
            tf[i] = "docs/inverted_index_term_freq_" + i + ".txt";
            id[i] = "docs/inverted_index_docids_" + i + ".txt";
        }

        String outputLex = "docs/lexicon_tot.bin";
        String outputDocids = "docs/inverted_index_docids.bin";
        String outputFreqs = "docs/inverted_index_freq.bin";
        File fileFreq = new File(outputFreqs);

        //implemento Set --> used for Lookup su Set o(1);
        Set<String> globalTerms = new HashSet<>(ht_lexicon.keySet());
        TreeSet<String> sortedTerms = new TreeSet<>(globalTerms);
        //LinkedHashSet: the fastest way to iterate over a hashset
        LinkedHashSet<String> termSet = new LinkedHashSet<>(sortedTerms);
        Iterator<String> itTerms = termSet.iterator(); //--> iterator for all term in collection
        Compressor compressor = new Compressor();

        BufferedWriter outLex = null;

        try {
            outLex = new BufferedWriter(new FileWriter(new File(outputLex)));
            RandomAccessFile streamDocs = new RandomAccessFile(new File(outputDocids), "rw");
            FileChannel channelDocs = streamDocs.getChannel();
            RandomAccessFile streamFreq = new RandomAccessFile(fileFreq, "rw");
            FileChannel channelFreq = streamFreq.getChannel();
            int docOffset = 0;
            int tfOffset = 0;
            //iterate through all term of collections
            while (itTerms.hasNext()) {
                //for(String lexTerm: termSet){
                String lexTerm = itTerms.next();
                String term = "";
                int nDocids = 0; //to count the docid list size in bytes
                int nFreqs = 0; //to count the term frequencies list size in bytes
                //iterate through all block
                for(int i = 0; i <= n; i++){
                    String line = ""; //term of the vocabulary
                    itLex[i] = Files.newBufferedReader(Paths.get(lex[i]), StandardCharsets.UTF_8);
                    itId[i] = Files.newBufferedReader(Paths.get(id[i]), StandardCharsets.UTF_8);
                    itTf[i] = Files.newBufferedReader(Paths.get(tf[i]), StandardCharsets.UTF_8);
                    //iterate through all lexicon of all block
                    while ((line = itLex[i].readLine()) != null) {
                        //splitted for the offset
                        //1,000,000 iterations of split take 3.36s,
                        //while 1,000,000 iterations of substring take only 0.05s.
                        term = line.substring(0, line.indexOf(" "));
                        String input = line.substring(line.indexOf(" ")+1, line.length());
                        int offset = Integer.parseInt(input);
                        //if a match is founded, a merge is made
                        //to reach the right line on files , an offset is used
                        if (lexTerm.equals(term)) {
                            //System.out.println(term);
                            //read the postings at the desired offset
                            String docLine = (String) FileUtils.readLines(new File(id[i]), "UTF-8").get(offset); //--> doc_id of selected term
                            String freqLine = (String) FileUtils.readLines(new File(tf[i]), "UTF-8").get(offset); //--> term freq of selected term
                            int countDoc = docLine.indexOf(" "); // in our text the doc_id are separated by white space (" ")
                            int countFreq = freqLine.indexOf(" ");
                            //we read the postings with substring because it's faster
                            if(countDoc == -1){ //if is not founded " " , mean that there is only one value , so a parsing is computed
                                int docid = Integer.parseInt(docLine);
                                int freq = Integer.parseInt(freqLine);
                                byte[] baDocs = compressor.stringCompressionWithVariableByte(docid);
                                ByteBuffer bufferValue = ByteBuffer.allocate(baDocs.length);
                                bufferValue.put(baDocs);
                                bufferValue.flip();
                                channelDocs.write(bufferValue);
                                byte[] baFreqs = compressor.stringCompressionWithLF(freq);
                                ByteBuffer bufferFreq = ByteBuffer.allocate(baFreqs.length);
                                bufferFreq.put(baFreqs);
                                bufferFreq.flip();
                                channelFreq.write(bufferFreq);
                                //TODO: write \n????
                                //System.out.println(docid + " " + baDocs.length);
                                nDocids+=baDocs.length;
                                nFreqs+=baFreqs.length;
                                break;
                            }
                            //iterate thought all doc_id of selected term
                            while(docLine!= ""){
                                int docid = Integer.parseInt(docLine.substring(0, countDoc));
                                int freq = Integer.parseInt(freqLine.substring(0,countFreq));
                                byte[] baDocs = compressor.stringCompressionWithVariableByte(docid);
                                ByteBuffer bufferValue = ByteBuffer.allocate(baDocs.length);
                                bufferValue.put(baDocs);
                                bufferValue.flip();
                                channelDocs.write(bufferValue);
                                byte[] baFreqs = compressor.stringCompressionWithLF(freq);
                                ByteBuffer bufferFreq = ByteBuffer.allocate(baFreqs.length);
                                bufferFreq.put(baFreqs);
                                bufferFreq.flip();
                                channelFreq.write(bufferFreq);
                                nDocids+=baDocs.length;
                                nFreqs+=baFreqs.length;
                                //System.out.println(docid + " " + baDocs.length);
                                String nextDoc = docLine.substring(docLine.indexOf(" ")+1); //--> next doc_id
                                String nextFreq = freqLine.substring(freqLine.indexOf(" ")+1); //--> next freq
                                //if there are other values, then the next posting is not the last
                                // and so take the next posting separated by space, otherwise take the last posting
                                countDoc = nextDoc.indexOf(" ") == -1 ? nextDoc.length()-1 : nextDoc.indexOf(" ");
                                countFreq = nextFreq.indexOf(" ") == -1 ? nextFreq.length()-1 : nextFreq.indexOf(" ");
                                //compression unary
                                docLine = nextDoc;
                                freqLine = nextFreq;
                            }
                            break;
                        }

                    }
                }
                //TODO 06/11/2022: write to the lexicon file the length of the posting list and the offset in BYTES!
                //countTerm++; //increment the offset
                //NEW LINE BIN FILE
                /*ByteBuffer bufferLine = ByteBuffer.allocate(2);
                bufferLine.put("\n".getBytes());
                bufferLine.flip();
                channel.write(bufferLine)*/;
                lexTerm += " " + docOffset + " " + nDocids + " " + tfOffset + " " + nFreqs;
                docOffset+=nDocids; //increment the offset
                tfOffset+=nFreqs;
                outLex.write(lexTerm);
                outLex.newLine();
                outLex.flush();
            }

        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {

            try {
                // always close the writer at the end of merging phase
                outLex.close();
            }
            catch (Exception e) {
            }
        }
    }



    /**
     * count the number of lines in the file
     * it should be the fastest way possible according to online benchmark
     * @param fileName_path
     * @return
     */
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
