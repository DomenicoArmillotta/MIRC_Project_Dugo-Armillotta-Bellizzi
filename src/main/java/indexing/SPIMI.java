package indexing;


import invertedIndex.InvertedIndex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import preprocessing.PreprocessDoc;

import java.nio.*;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class SPIMI {

    private DB db;
    private HTreeMap<String, Integer> documentIndex;

    private InvertedIndex invertedIndex;
    private String outPath;

    private int docid = 0;


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

    /*private void mergeBlocks(int n) throws IOException {
        TreeSet<String> treeSet = new TreeSet<>();
        int bytesRead = 0;
        boolean isTerm = true;
        int[] isCompleted = new int[n];
        //tantissimi channel
        FileChannel[] listOfChannel = new FileChannel[n];
        for (int i = 0; i<n; i++) {
            Path path = Paths.get("ouptut_" + i + ".txt");
            listOfChannel[i] = FileChannel.open(path, StandardOpenOption.READ);
            //tantissimi buffer
        }

        long[] pos = new long[n];
        while (isTerm == true){
            for(int j=0;j<n;j++){
                //è stato creato un array di dimensione n , se nella posizione c'è  = 1 il file ha finito i termini altrimenti li ha ancora
                if(isCompleted[j]==0){
                    //accediamo al file j
                    listOfChannel[j].position(pos[j]);
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    listOfChannel[j].read(buffer);
                    buffer.flip();
                    //qui stiamo leggendo i primi 1024 byte del file
                    for (int k = 0; k<n; )

                }
            }
        }
    }*/




    public void spimiInvert(List<String> fileBlock, int n) throws IOException {

    }






}




