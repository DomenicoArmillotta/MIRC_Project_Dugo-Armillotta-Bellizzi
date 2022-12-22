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

import static utility.Utils.addByteArray;

public class SPIMI {

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
        int indexBlock = 0;
        //int cont = 0;
        try {
            //create chunk of data , splitting in n different block
            while (it.hasNext()){
                //instantiate a new Inverted Index and Lexicon per block
                invertedIndex = new InvertedIndex(indexBlock);
                outPath = "index"+indexBlock+".txt";
                while (it.hasNext() && Runtime.getRuntime().totalMemory()*0.80 <= Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()) {
                    //--> its the ram of jvm
                    String line = it.nextLine();
                    spimiInvertMapped(line);
                    //System.out.println(cont);
                    //cont++;
                }
                //System.out.println(cont);
                invertedIndex.sortTerms();
                invertedIndex.writePostings();
                indexBlock++;
                System.gc();
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        db.commit();
        db.close();
        //FARE MERGE dei VARI BLOCCHI qui
        mergeBlocks(indexBlock);
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
        //per lettura
        List<String> lexPaths = new ArrayList<>();
        List<String> docPaths = new ArrayList<>();
        List<String> tfPaths = new ArrayList<>();
        for(int i = 0; i < n; i++){
            lexPaths.add("docs/lexicon"+i+".txt");
            docPaths.add("docs/docids"+i+".txt");
            tfPaths.add("docs/tfs"+i+".txt");
        }
        /*FileChannel[] lexChannels = new FileChannel[n];
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
        }*/
        /*List<FileChannel> lexChannels = new ArrayList<>();
        List<FileChannel> docChannels = new ArrayList<>();
        List<FileChannel> tfChannels = new ArrayList<>();
        List<RandomAccessFile> lexFiles = new ArrayList<>();
        List<RandomAccessFile> docFiles = new ArrayList<>();
        List<RandomAccessFile> tfFiles = new ArrayList<>();
        List<String> lexPaths = new ArrayList<>();
        List<String> docPaths = new ArrayList<>();
        List<String> tfPaths = new ArrayList<>();
        for(int i = 0; i < n; i++){
            lexPaths.add("docs/lexicon"+i+".txt");
            docPaths.add("docs/docids"+i+".txt");
            tfPaths.add("docs/tfs"+i+".txt");
            lexFiles.add(new RandomAccessFile(new File("docs/lexicon"+i+".txt"), "rw"));
            docFiles.add(new RandomAccessFile(new File("docs/docids"+i+".txt"), "rw"));
            tfFiles.add(new RandomAccessFile(new File("docs/tfs"+i+".txt"), "rw"));
            lexChannels.add(lexFiles.get(i).getChannel());
            docChannels.add(docFiles.get(i).getChannel());
            tfChannels.add(tfFiles.get(i).getChannel());
        }*/
        //per scrittura
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

        //Buffer per leggere ogni termine con annesse statistiche
        ByteBuffer[] readBuffers = new ByteBuffer[n];
        //ByteBuffer buffer = ByteBuffer.allocate(58*termsNumber);
        //IDEA: tenere una variabile che conta quanti blocchi sono rimasti
        List<String> currLex = lexPaths;
        List<String> currDocs = docPaths;
        List<String > currTfs = tfPaths;
        int nIndex = n;
        //System.out.println("HERE " + nIndex);
        while(nIndex>1){
            System.out.println("HERE " + nIndex);
            //inizializzare una variabile per indicizzare il numero del file intermedio, in modo tale che ad ogni
            //for abbiamo il numero di file intermedi creat e all'inizio di una nuova iterazione del while, lo rimettiamo
            // a zero per segnarci i nuovi indici dei nuovi file intermedi
            List<String> tempLex = new ArrayList<>();
            List<String> tempDocs = new ArrayList<>();
            List<String> tempTfs = new ArrayList<>();
            int nFile = 0;
            long totalSize = 0; //we need to keep track of the total length of the file(s) to merge;
            //when total_size is equal to the sum of the lengths of the files, we have finished to read
            for (int i = 0; i<nIndex; i+=2){
                //output files
                String docPath = "docs/tempD"+nFile+".txt";
                String tfPath = "docs/tempT"+nFile+".txt";
                String lexPath = "docs/tempL"+nFile+".txt";
                RandomAccessFile outDocsFile = new RandomAccessFile(new File(docPath),"rw");
                FileChannel tempDocChannel = outDocsFile.getChannel();
                RandomAccessFile outTfFile = new RandomAccessFile(new File(tfPath),"rw");
                FileChannel tempTfChannel = outTfFile.getChannel();
                RandomAccessFile outFile = new RandomAccessFile(new File(lexPath),"rw");
                FileChannel tempChannel = outFile.getChannel();
                if(i == nIndex-1) { //there are no other blocks to merge
                    //in this case we need to copy the filename in the new list of paths
                    tempLex.add(currLex.get(i));
                    tempDocs.add(currDocs.get(i));
                    tempTfs.add(currTfs.get(i));
                }
                //controlla che ci sia un altro blocco o meno e in quel caso non mergiare
                //altrimenti:
                else{
                    //declare input files
                    RandomAccessFile doc1File = new RandomAccessFile(new File(currDocs.get(i)),"rw");
                    FileChannel doc1Channel = doc1File.getChannel();
                    RandomAccessFile tf1File = new RandomAccessFile(new File(currTfs.get(i)),"rw");
                    FileChannel tf1Channel = tf1File.getChannel();
                    RandomAccessFile lex1File = new RandomAccessFile(new File(currLex.get(i)),"rw");
                    FileChannel lex1Channel = lex1File.getChannel();
                    RandomAccessFile doc2File = new RandomAccessFile(new File(currDocs.get(i+1)),"rw");
                    FileChannel doc2Channel = doc2File.getChannel();
                    RandomAccessFile tf2File = new RandomAccessFile(new File(currTfs.get(i+1)),"rw");
                    FileChannel tf2Channel = tf2File.getChannel();
                    RandomAccessFile lex2File = new RandomAccessFile(new File(currLex.get(i+1)),"rw");
                    FileChannel lex2Channel = lex2File.getChannel();
                    //for reading we use the pointers with the position method, so we need the offsets of the two files
                    //offsets for reading the two files
                    long offset1 = 0;
                    long offset2 = 0;
                    long docOffset = 0; //offset of the docids list in the docids output file
                    long tfOffset = 0; //offset of the tfs list in the tfs output file
                    while(totalSize < lex1File.length() + lex2File.length()){
                        readBuffers[i] = ByteBuffer.allocate(LEXICON_ENTRY_SIZE);
                        readBuffers[i+1] = ByteBuffer.allocate(LEXICON_ENTRY_SIZE);
                        //we set the position in the files using the offsets
                        lex1Channel.position(offset1);
                        lex2Channel.position(offset2);
                        lex1Channel.read(readBuffers[i]);
                        lex2Channel.read(readBuffers[i+1]);
                        //next steps:
                        //1)read the term in both buffers (first 22 bytes) and the lexicon statistics (remaining 36);
                        //read first 22 bytes for the term
                        byte[] term1 = readBuffers[i].get(readBuffers[i].array(), 0, 22).array();
                        byte[] term2 = readBuffers[i+1].get(readBuffers[i+1].array(), 0, 22).array();
                        //read remaining bytes for the lexicon stats
                        ByteBuffer val1 = readBuffers[i].get(readBuffers[i].array(), 22, LEXICON_ENTRY_SIZE-22);
                        ByteBuffer val2 = readBuffers[i+1].get(readBuffers[i+1].array(), 22, LEXICON_ENTRY_SIZE-22);
                        //we use a method for reading the 36 bytes in a LexiconStats object
                        LexiconStats l1 = new LexiconStats(val1);
                        LexiconStats l2 = new LexiconStats(val2);
                        //convert the bytes to the String
                        String word1 = Text.decode(readBuffers[i].array());
                        String word2 = Text.decode(readBuffers[i+1].array());
                        //2)compare terms to see what to merge in the result
                        //3)check:
                        //if the 1st term is greater than the second
                        //if the second is greater than the other
                        //if they are equal: in this case we merge them
                        //in the LexiconStats
                        if(word1.compareTo(word2) > 0){
                            ByteBuffer docids = ByteBuffer.allocate(l1.getDocidsLen());
                            ByteBuffer tfs = ByteBuffer.allocate(l1.getTfLen());
                            doc1Channel.position(l1.getOffsetDocid());
                            tf1Channel.position(l1.getOffsetTf());
                            doc1Channel.read(docids);
                            tf1Channel.read(tfs);
                            docids.flip();
                            tempDocChannel.write(docids);
                            tfs.flip();
                            tempTfChannel.write(tfs);
                            byte[] lexiconBytes;
                            lexiconBytes = ByteBuffer.allocate(22).put(term1).array();
                            //take the document frequency
                            byte[] dfBytes = ByteBuffer.allocate(4).putInt(l1.getdF()).array();
                            //take the collection frequency
                            byte[] cfBytes = ByteBuffer.allocate(8).putLong(l1.getCf()).array();
                            //take list dim for both docids and tfs
                            byte[] docBytes = ByteBuffer.allocate(4).putInt(l1.getDocidsLen()).array();
                            byte[] tfBytes = ByteBuffer.allocate(4).putInt(l1.getTfLen()).array();
                            //take the offset of docids
                            byte[] offsetDocBytes = ByteBuffer.allocate(8).putLong(docOffset).array();
                            //take the offset of tfs
                            byte[] offsetTfBytes = ByteBuffer.allocate(8).putLong(tfOffset).array();
                            //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
                            lexiconBytes = addByteArray(lexiconBytes,dfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,cfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,docBytes);
                            lexiconBytes = addByteArray(lexiconBytes,tfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
                            //write lexicon entry to disk
                            ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
                            bufferLex.put(lexiconBytes);
                            bufferLex.flip();
                            tempChannel.write(bufferLex);
                            //update offsets
                            docOffset+=l1.getDocidsLen();
                            tfOffset+=l1.getTfLen();
                            offset1+= LEXICON_ENTRY_SIZE;
                            totalSize+=LEXICON_ENTRY_SIZE;
                        }
                        else if(word2.compareTo(word1) > 0){
                            ByteBuffer docids = ByteBuffer.allocate(l2.getDocidsLen());
                            ByteBuffer tfs = ByteBuffer.allocate(l2.getTfLen());
                            doc2Channel.position(l2.getOffsetDocid());
                            tf2Channel.position(l2.getOffsetTf());
                            doc2Channel.read(docids);
                            tf2Channel.read(tfs);
                            docids.flip();
                            tempDocChannel.write(docids);
                            tfs.flip();
                            tempTfChannel.write(tfs);
                            byte[] lexiconBytes;
                            lexiconBytes = ByteBuffer.allocate(22).put(term2).array();
                            //take the document frequency
                            byte[] dfBytes = ByteBuffer.allocate(4).putInt(l2.getdF()).array();
                            //take the collection frequency
                            byte[] cfBytes = ByteBuffer.allocate(8).putLong(l2.getCf()).array();
                            //take list dim for both docids and tfs
                            byte[] docBytes = ByteBuffer.allocate(4).putInt(l2.getDocidsLen()).array();
                            byte[] tfBytes = ByteBuffer.allocate(4).putInt(l2.getTfLen()).array();
                            //take the offset of docids
                            byte[] offsetDocBytes = ByteBuffer.allocate(8).putLong(docOffset).array();
                            //take the offset of tfs
                            byte[] offsetTfBytes = ByteBuffer.allocate(8).putLong(tfOffset).array();
                            //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
                            lexiconBytes = addByteArray(lexiconBytes,dfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,cfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,docBytes);
                            lexiconBytes = addByteArray(lexiconBytes,tfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
                            //write lexicon entry to disk
                            ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
                            bufferLex.put(lexiconBytes);
                            bufferLex.flip();
                            tempChannel.write(bufferLex);
                            //update offsets
                            docOffset+=l2.getDocidsLen();
                            tfOffset+=l2.getTfLen();
                            offset2+=LEXICON_ENTRY_SIZE;
                            totalSize+=LEXICON_ENTRY_SIZE;
                        }
                        else if (word1.compareTo(word2) == 0){
                            //3.5) MERGE: we merge the docids, the tfs in the files; we merge the dF, cF, docidslen and tfslen
                            //4) in the new merged files we need to write the term and the new lexicon stats with the
                            //updated stats (offsets etc..)
                            ByteBuffer docids1 = ByteBuffer.allocate(l1.getDocidsLen());
                            ByteBuffer tfs1 = ByteBuffer.allocate(l1.getTfLen());
                            doc1Channel.position(l1.getOffsetDocid());
                            tf1Channel.position(l1.getOffsetTf());
                            doc1Channel.read(docids1);
                            tf1Channel.read(tfs1);
                            docids1.flip();
                            tempDocChannel.write(docids1);
                            tfs1.flip();
                            tempTfChannel.write(tfs1);
                            ByteBuffer docids2 = ByteBuffer.allocate(l2.getDocidsLen());
                            ByteBuffer tfs2 = ByteBuffer.allocate(l2.getTfLen());
                            doc2Channel.position(l2.getOffsetDocid());
                            tf2Channel.position(l2.getOffsetTf());
                            doc2Channel.read(docids2);
                            tf2Channel.read(tfs2);
                            docids2.flip();
                            tempDocChannel.write(docids2);
                            tfs2.flip();
                            tempTfChannel.write(tfs2);
                            int docLen = l1.getDocidsLen()+l2.getDocidsLen();
                            int tfLen = l1.getTfLen()+l2.getTfLen();
                            byte[] lexiconBytes;
                            lexiconBytes = ByteBuffer.allocate(22).put(term1).array();
                            //take the document frequency
                            byte[] dfBytes = ByteBuffer.allocate(4).putInt(l1.getdF()+l2.getdF()).array();
                            //take the collection frequency
                            byte[] cfBytes = ByteBuffer.allocate(8).putLong(l1.getCf()+l2.getCf()).array();
                            //take list dim for both docids and tfs
                            byte[] docBytes = ByteBuffer.allocate(4).putInt(docLen).array();
                            byte[] tfBytes = ByteBuffer.allocate(4).putInt(tfLen).array();
                            //take the offset of docids
                            byte[] offsetDocBytes = ByteBuffer.allocate(8).putLong(docOffset).array();
                            //take the offset of tfs
                            byte[] offsetTfBytes = ByteBuffer.allocate(8).putLong(tfOffset).array();
                            //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
                            lexiconBytes = addByteArray(lexiconBytes,dfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,cfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,docBytes);
                            lexiconBytes = addByteArray(lexiconBytes,tfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
                            //write lexicon entry to disk
                            ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
                            bufferLex.put(lexiconBytes);
                            bufferLex.flip();
                            tempChannel.write(bufferLex);
                            //update offsets
                            docOffset+=docLen;
                            tfOffset+=tfLen;
                            offset1 += LEXICON_ENTRY_SIZE;
                            offset2 += LEXICON_ENTRY_SIZE;
                            totalSize+=LEXICON_ENTRY_SIZE*2; //we read two entries in total
                        }
                        //add output files to paths
                        tempLex.add(lexPath);
                        tempDocs.add(docPath);
                        tempTfs.add(tfPath);
                    }
                }
                nFile++;
            }
            //update file paths: first clear old paths, then update with new paths
            currDocs.clear();
            currTfs.clear();
            currLex.clear();
            currDocs = tempDocs;
            currTfs = tempTfs;
            currLex = tempLex;
            nIndex = (int) Math.ceil((double)nIndex/2); //attenzione all'approssimazione nel caso di numero di blocchi dispari
        }
        //TODO: write the output files
    }



    public void spimiInvert(List<String> fileBlock, int n) throws IOException {

    }



}




