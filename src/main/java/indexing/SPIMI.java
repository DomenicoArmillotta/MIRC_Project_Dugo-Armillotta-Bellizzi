package indexing;


import fileManager.ConfigurationParameters;
import invertedIndex.InvertedIndex;
import invertedIndex.LexiconStats;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.C;
import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import preprocessing.PreprocessDoc;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static utility.Utils.addByteArray;

public class SPIMI {
    private InvertedIndex invertedIndex;
    private String outPath;
    private int docid = 0;


    //creazione dei blocchi usando il limite su ram
    public void spimiInvertBlockMapped(String readPath) throws IOException {
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
                while (it.hasNext()){
                    String line = it.nextLine();
                    spimiInvertMapped(line);
                    if(Runtime.getRuntime().totalMemory()*0.80 >
                            Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()){
                        //--> its the ram of jvm
                        break;
                    }
                }
                //vecchio controllo
                /*while (it.hasNext() && Runtime.getRuntime().totalMemory()*0.80 <= Runtime.getRuntime().maxMemory() - Runtime.getRuntime().totalMemory() + Runtime.getRuntime().freeMemory()) {
                    //--> its the ram of jvm
                    String line = it.nextLine();
                    spimiInvertMapped(line);
                    //System.out.println(cont);
                    //cont++;
                }*/
                invertedIndex.sortTerms();
                invertedIndex.writePostings();
                indexBlock++;
                System.gc();
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        mergeBlocks(indexBlock);
    }

    public void spimiInvertMapped(String doc) throws IOException {
        //initialize a new InvertedIndex
        PreprocessDoc preprocessDoc = new PreprocessDoc();
        String[] parts = doc.split("\t");
        String doc_corpus = parts[1];
        List<String> pro_doc = preprocessDoc.preprocess_doc_optimized(doc_corpus);
        //read the terms and generate postings
        for (String term : pro_doc) {
            invertedIndex.addPosting(term, docid, 1);
        }
        docid++;
    }

    private void mergeBlocks(int n) throws IOException {
        //per lettura
        List<String> lexPaths = new ArrayList<>();
        List<String> docPaths = new ArrayList<>();
        List<String> tfPaths = new ArrayList<>();
        for(int i = 0; i <= n; i++){
            lexPaths.add("docs/lexicon_"+i+".txt");
            docPaths.add("docs/docids_"+i+".txt");
            tfPaths.add("docs/tfs_"+i+".txt");
        }
        //take the entry size of the lexicon from the configuration parameters
        int LEXICON_ENTRY_SIZE = ConfigurationParameters.LEXICON_ENTRY_SIZE;
        //Buffer per leggere ogni termine con annesse statistiche
        ByteBuffer[] readBuffers = new ByteBuffer[n];
        //ByteBuffer buffer = ByteBuffer.allocate(58*termsNumber);
        //IDEA: tenere una variabile che conta quanti blocchi sono rimasti
        List<String> currLex = lexPaths;
        List<String> currDocs = docPaths;
        List<String> currTfs = tfPaths;
        int nIndex = n;
        ConfigurationParameters cp = new ConfigurationParameters();
        double N = cp.getNumberOfDocuments(); //take the total number of documents in the collection
        //System.out.println("HERE " + nIndex);
        //in the case of multiple block to merge
        while(nIndex>1){
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
                        //1)read the term in both buffers (first 22 bytes) and the lexicon statistics (remaining 44);
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
                        /*String word1 = Text.decode(readBuffers[i].array());
                        String word2 = Text.decode(readBuffers[i+1].array());*/
                        String word1 = Text.decode(term1).toString();
                        String word2 = Text.decode(term2).toString();
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
                            //idf value
                            long nn = l1.getdF(); // number of documents that contain the term t among the data set
                            double idf = Math.log((N/nn));
                            byte[] idfBytes = ByteBuffer.allocate(8).putDouble(idf).array();
                            //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
                            lexiconBytes = addByteArray(lexiconBytes,dfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,cfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,docBytes);
                            lexiconBytes = addByteArray(lexiconBytes,tfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,idfBytes);
                            //write lexicon entry to disk
                            ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
                            bufferLex.put(lexiconBytes);
                            bufferLex.flip();
                            tempChannel.write(bufferLex); //write in lexicon file
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
                            //idf value
                            long nn = l2.getdF(); // number of documents that contain the term t among the data set
                            double idf = Math.log((N/nn));
                            byte[] idfBytes = ByteBuffer.allocate(8).putDouble(idf).array();
                            //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
                            lexiconBytes = addByteArray(lexiconBytes,dfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,cfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,docBytes);
                            lexiconBytes = addByteArray(lexiconBytes,tfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,idfBytes);
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
                            //idf value
                            long nn = l1.getdF()+l2.getdF(); // number of documents that contain the term t among the data set
                            double idf = Math.log((N/nn));
                            byte[] idfBytes = ByteBuffer.allocate(8).putDouble(idf).array();
                            //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
                            lexiconBytes = addByteArray(lexiconBytes,dfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,cfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,docBytes);
                            lexiconBytes = addByteArray(lexiconBytes,tfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
                            lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
                            lexiconBytes = addByteArray(lexiconBytes,idfBytes);
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
        //Writing the output files
        File lexFile = new File("docs/lexicon.txt");
        File docFile = new File("docs/docids.txt");
        File tfFile = new File("docs/tfs.txt");
        FileOutputStream streamLex = new FileOutputStream(lexFile);
        FileOutputStream streamDocs = new FileOutputStream(docFile);
        FileOutputStream streamTf = new FileOutputStream(tfFile);
        WritableByteChannel lexChannel = streamLex.getChannel();
        WritableByteChannel docChannel = streamDocs.getChannel();
        WritableByteChannel tfChannel = streamTf.getChannel();
        //declare the input channels
        FileInputStream inputDocFile = new FileInputStream(new File(currDocs.get(0)));
        FileChannel inputDocChannel = inputDocFile.getChannel();
        FileInputStream inputTfFile = new FileInputStream(new File(currTfs.get(0)));
        FileChannel inputTfChannel = inputTfFile.getChannel();
        FileInputStream inputLexFile = new FileInputStream(new File(currLex.get(0)));
        FileChannel inputLexChannel = inputLexFile.getChannel();
        //we use the method transferTo to copy the file from a channel to another
        inputDocChannel.transferTo(0, inputDocChannel.size(), docChannel);
        inputTfChannel.transferTo(0, inputTfChannel.size(), tfChannel);
        inputLexChannel.transferTo(0, inputLexChannel.size(), lexChannel);
    }

}




