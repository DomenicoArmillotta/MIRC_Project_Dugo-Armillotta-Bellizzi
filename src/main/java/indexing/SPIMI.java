package indexing;


import invertedIndex.InvertedIndex;
import invertedIndex.LexiconStats;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;
import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import preprocessing.PreprocessDoc;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class SPIMI implements Comparable<String> {

    private DB db;
    private HTreeMap<String, Integer> documentIndex;
    private InvertedIndex invertedIndex;
    private String outPath;
    private int docid = 0;
    private final int LEXICON_ENTRY_SIZE = 58;


    //creazione dei blocchi usando il limite su ram
    public void spimiInvertBlockMapped(String readPath) throws IOException {
        db = DBMaker.fileDB("docs/docIndex.db").make();
        documentIndex = db
                .hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
        File inputFile = new File(readPath);
        LineIterator it = FileUtils.lineIterator(inputFile, "UTF-8");
        int index_block = 0;
        //int cont = 0;
        try {
            //create chunk of data , splitting in n different block
            while (it.hasNext()){
                //instantiate a new Inverted Index and Lexicon per block
                invertedIndex = new InvertedIndex(index_block);
                outPath = "index"+index_block+".txt";
                while (it.hasNext() && Runtime.getRuntime().totalMemory()*0.80 <= Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) {
                    //--> its the ram of jvm
                    String line = it.nextLine();
                    spimiInvertMapped(line);
                    //System.out.println(cont);
                    //cont++;
                }
                //System.out.println(cont);
                invertedIndex.sortTerms();
                invertedIndex.writePostings();
                index_block++;
                System.gc();
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        db.commit();
        db.close();
        //FARE MERGE dei VARI BLOCCHI qui
    }

    public void spimiInvertMapped(String doc) throws IOException {
        //initialize a new InvertedIndex
        PreprocessDoc preprocessDoc = new PreprocessDoc();
        int cont = 0;
        String[] parts = doc.split("\t");
        String docno = parts[0];
        String doc_corpus = parts[1];
        List<String> pro_doc = preprocessDoc.preprocess_doc_optimized(doc_corpus);
        //read the terms and generate postings
        //write postings
        for (String term : pro_doc) {
            invertedIndex.addPosting(term, docid, 1);
            cont++;
        }
        documentIndex.put(docno, cont);
        docid++;
    }

       /* private void mergeBlocks(int n) throws IOException {
            int termsNumber = 100;
            byte[] partOfByteBuffer = new byte[20];
            TreeSet<String> terms = new TreeSet<>();
            FileChannel[] fileChannels = new FileChannel[n];
            long[] pos = new long[n];
            int[] cicleControl = new int[n];

            Arrays.fill(pos,0);
            Arrays.fill(cicleControl,0);

            ByteBuffer buffer = ByteBuffer.allocate(58*termsNumber);
            ByteBuffer buff = ByteBuffer.allocate(1024);

            int bytesRead = 0;
            for (int i = 0; i<n; i++){
                Path path = Paths.get("path_" + i + ".txt");
                fileChannels[i].open(path, StandardOpenOption.READ);
                bytesRead = fileChannels[i].read(buffer);
                buffer.flip();
                cicleControl[i] = (int) Math.ceil(fileChannels[i].size() / (58 * termsNumber));
                int k = 0;
                int controlSum=0;
                controlSum += cicleControl[i];
                while (controlSum != 0){
                    for (int l = 0; l < n; l++){
                        if (cicleControl[i] != 0){
                            fileChannels[l].position(pos[i]);
                            fileChannels[l].read(buffer);
                            for (int d = 0; d < 100; d++){
                                buffer.position(58*d);
                                byte[] arr = new byte[buffer.remaining()];
                                ByteBuffer term = buff.put(arr, 58 * d, 20);
                                byte[] bytes = new byte[term.remaining()];
                                String fTerm = new String(bytes, StandardCharsets.UTF_8);
                                terms.add(fTerm);
                            }
                        /*buffer.flip();
                        byte term = buffer.get();
                        //inserire nella prirityqueue
                        buffer.compact();

                        }
                    }
                    controlSum = controlSum - cicleControl[i];
                }
            }
        }*/

    private void mergeBlocks(int n) throws IOException {
        int termsNumber = 100;
        byte[] byteForTerm = new byte[22];
        //per lettura
        FileChannel[] lexChannels = new FileChannel[n];
        FileChannel[] docChannels = new FileChannel[n];
        FileChannel[] tfChannels = new FileChannel[n];
        RandomAccessFile[] lexFiles = new RandomAccessFile[n];
        RandomAccessFile[] docFiles = new RandomAccessFile[n];
        RandomAccessFile[] tfFiles = new RandomAccessFile[n];
        for(int i = 0; i < n; i++){
            lexFiles[i] = new RandomAccessFile(new File("docs/lexicon"+i+".txt"), "rw");
            docFiles[i] = new RandomAccessFile(new File("docs/docids"+i+".txt"), "rw");
            tfFiles[i] = new RandomAccessFile(new File("docs/tfs"+i+".txt"), "rw");
            lexChannels[i] = lexFiles[i].getChannel();
            docChannels[i] = docFiles[i].getChannel();
            tfChannels[i] = tfFiles[i].getChannel();
        }

        //per scrittura
        FileOutputStream fOut;
        File lexFile = new File("docs/lexicon.txt");
        File docFile = new File("docs/docids.txt");
        File tfFile = new File("docs/tfs.txt");
        RandomAccessFile streamLex = new RandomAccessFile(lexFile, "rw");
        RandomAccessFile streamDocs = new RandomAccessFile(docFile, "rw");
        RandomAccessFile streamTf = new RandomAccessFile(tfFile, "rw");
        FileChannel lexChannel = streamLex.getChannel();
        FileChannel docChannel = streamDocs.getChannel();
        FileChannel tfChannel = streamTf.getChannel();
        ByteBuffer mBuf;

        //Buffer per leggere ogni termini con annesse statistiche
        ByteBuffer[] readBuffers = new ByteBuffer[n];
        //ByteBuffer buffer = ByteBuffer.allocate(58*termsNumber);
        //IDEA: tenere una variabile che conta quanti blocchi sono rimasti
        int nIndex = n;
        while(nIndex>1){
            //inizializzare una variabile per indicizzare il numero del file intermedio, in modo tale che ad ogni
            //for abbiamo il numero di file intermedi creat e all'inizio di una nuova iterazione del while, lo rimettiamo
            // a zero per segnarci i nuovi indici dei nuovi file intermedi
            int nFile = 0;
            long totalSize = 0; //we need to keep track of the total length of the file(s) to merge;
            //when total_size is equal to the sum of the lengths of the files, we have finished to read
            for (int i = 0; i<nIndex; i+=2){
                //controlla che ci sia un altro blocco o meno e in quel caso non mergiare
                if(i == nIndex-1) { //there are no other blocks to merge
                    //in this case we need to copy the file
                    while(totalSize < lexFiles[i].length()){
                        readBuffers[i] = ByteBuffer.allocate(LEXICON_ENTRY_SIZE);
                    }
                }
                //altrimenti:
                else{
                    //for reading we use the pointers with the position method, so we need the offsets of the two files
                    //offsets for reading the two files
                    long offset1 = 0;
                    long offset2 = 0;
                    //length of the bytes read in the two files so far --> not needed with FileChannel
                    int length1 = 0;
                    int length2 = 0;
                    while(totalSize < lexFiles[i].length() + lexFiles[i+1].length()){
                        readBuffers[i] = ByteBuffer.allocate(LEXICON_ENTRY_SIZE);
                        readBuffers[i+1] = ByteBuffer.allocate(LEXICON_ENTRY_SIZE);
                        lexChannels[i].position(offset1);
                        lexChannels[i+1].position(offset2);
                        lexChannels[i].read(readBuffers[i]);
                        lexChannels[i+1].read(readBuffers[i+1]);
                        //next steps:
                        //1)read the term in both buffers (first 22 bytes) and the lexicon statistics (remaining 36);
                        //for simplicity we can do a method for reading the 36 bytes in a LexiconStats object
                        //2)compare terms to see what to merge in the result
                        //the result is a temp randomaccessfile
                        //3)check:
                            //if the 1st term is greater than the second
                            //if the second is greater than the other
                            //if they are equal: in this case we merge them
                        //3.5) MERGE: we merge the docids, the tfs in the files; we merge the dF, cF, docidslen and tfslen
                        //in the LexiconStats
                        //4) in the new merged files we need to write the term and the new lexicon stats with the
                        //updated stats (offsets etc..)
                        //TODO: check if we have read both entries or not; if not, we increase totalSize by 58,
                        // otherwise by 58*2
                        totalSize+=LEXICON_ENTRY_SIZE;
                    }
                    //merge dei blocchi --> è il merge di mergesort
                }
                //ATTENZIONE!!!!! quando i due elementi (termini) sono uguali, si scorre di una posizione entrambi i buffer
                //scrivi il blocco mergiato in un nuovo file
                //quindi per ogni iterazione dichiariamo un file temp in cui scrivere il file intermedio
                nFile++;
            }
            nIndex = nIndex/2; //attenzione all'approsimazione nel caso di numero di blocchi dispari
        }
        //il codice qua sotto è sbagliato
        /*for (int i = 0; i<n; i+=2){
            Path lexPath = Paths.get("lexicon_" + i + ".txt");
            Path docPath = Paths.get("docids_" + i + ".txt");
            Path tfPath = Paths.get("tfs_" + i + ".txt");
            lexChannels[i].open(lexPath, StandardOpenOption.READ);
            lexChannels[i+1].open(lexPath, StandardOpenOption.READ);
            readBuffers[i] = ByteBuffer.allocate(22);
            readBuffers[i+1] = ByteBuffer.allocate(22);
            lexChannels[i].read(readBuffers[i]);
            lexChannels[i+1].read(readBuffers[i+1]);
            String row1 = Text.decode(readBuffers[i].array());
            String row2 = Text.decode(readBuffers[i+1].array());
            byte[] term1;
            byte[] term2;
            if(row1.getLength()>=21 && row2.getLength()>=21){
                Text truncatedTerm1 = new Text(row1.toString().substring(0,20));
                Text truncatedTerm2 = new Text(row2.toString().substring(0,20));
                term1 = truncatedTerm1.getBytes();
                term2 = truncatedTerm2.getBytes();
                String stringTerm1 = new String(term1, StandardCharsets.UTF_8);
                String stringTerm2 = new String(term2, StandardCharsets.UTF_8);
            }
        }*/

    }


    @Override
    public int compareTo(@NotNull String term) {
        return 0;
    }







    public void spimiInvert(List<String> fileBlock, int n) throws IOException {

    }



}




