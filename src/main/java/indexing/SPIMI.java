package indexing;


import invertedIndex.InvertedIndex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
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




    private void mergeBlocks(int n) throws IOException {
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
                        buffer.compact();*/

                    }
                }
                controlSum = controlSum - cicleControl[i];
            }
        }







    }




    public void spimiInvert(List<String> fileBlock, int n) throws IOException {

    }






}




