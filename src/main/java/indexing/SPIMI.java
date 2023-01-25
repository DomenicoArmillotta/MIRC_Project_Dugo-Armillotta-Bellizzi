package indexing;


import compression.Compressor;
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
import queryProcessing.Scorer;
import utility.Utils;

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
        System.out.println(docid);
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
        //in the case of multiple block to merge
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
                        //place the pointers in the buffers at the beginning
                        readBuffers[i].position(0);
                        readBuffers[i+1].position(0);
                        //next steps:
                        //1)read the term in both buffers (first 22 bytes) and the lexicon statistics (remaining 44);
                        //read first 22 bytes for the term
                        byte[] term1 = new byte[22];
                        byte[] term2 = new byte[22];
                        readBuffers[i].get(term1, 0, 22).array();
                        readBuffers[i+1].get(term2, 0, 22).array();
                        //read remaining bytes for the lexicon stats
                        ByteBuffer val1 = ByteBuffer.allocate(LEXICON_ENTRY_SIZE-22);
                        ByteBuffer val2 = ByteBuffer.allocate(LEXICON_ENTRY_SIZE-22);
                        readBuffers[i].get(val1.array(), 0, LEXICON_ENTRY_SIZE-22);
                        readBuffers[i+1].get(val2.array(), 0, LEXICON_ENTRY_SIZE-22);
                        //we use a method for reading the 36 bytes in a LexiconStats object
                        LexiconStats l1 = new LexiconStats(val1);
                        LexiconStats l2 = new LexiconStats(val2);
                        //convert the bytes to the String
                        String word1 = Text.decode(term1);
                        String word2 = Text.decode(term2);
                        //replace null characters
                        word1 = word1.replaceAll("\0", "");
                        word2 = word2.replaceAll("\0", "");
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
                            byte[] lexiconBytes = Utils.getBytesFromString(word1);
                            //idf value
                            long nn = l1.getdF(); // number of documents that contain the term t among the data set
                            double idf = Math.log((N/nn));
                            int docLen = l1.getDocidsLen();
                            int tfLen = l1.getTfLen();
                            int df = l1.getdF();
                            long cF = l1.getCf();
                            lexiconBytes = addByteArray(lexiconBytes, Utils.createLexiconEntry(df, cF, docLen,tfLen, docOffset, tfOffset, idf, 0.0, 0.0, 0, 0));
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
                            byte[] lexiconBytes = Utils.getBytesFromString(word2);
                            //idf value
                            long nn = l2.getdF(); // number of documents that contain the term t among the data set
                            double idf = Math.log((N/nn));
                            int docLen = l2.getDocidsLen();
                            int tfLen = l2.getTfLen();
                            int df = l2.getdF();
                            long cF = l2.getCf();
                            lexiconBytes = addByteArray(lexiconBytes, Utils.createLexiconEntry(df, cF, docLen,tfLen, docOffset, tfOffset, idf, 0.0, 0.0, 0, 0));
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
                            byte[] lexiconBytes = Utils.getBytesFromString(word1);
                            //idf value
                            long nn = l1.getdF()+l2.getdF(); // number of documents that contain the term t among the data set
                            double idf = Math.log((N/nn));
                            int df = l1.getdF()+l2.getdF();
                            long cF = l1.getCf()+l2.getCf();
                            lexiconBytes = addByteArray(lexiconBytes, Utils.createLexiconEntry(df, cF, docLen, tfLen, docOffset, tfOffset, idf, 0.0, 0.0, 0, 0));
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

    public void computeMaxScores() throws IOException {
        //questo metodo legge il lexicon un termine alla volta; per ogni termine calcola la term upper bound leggendo la lista;
        //serve anche aprire il doc index per la document length; si chiama bm25 per ogni doc della lista e si prende il massimo
        //punteggio e si scrive sul file del lexicon
        //SKIP BLOCKS:
        //aggiungere nel lexicon la lunghezza dell'header (int)
        //per sapere quante coppie (b,endocid) sono basta fare la radice di docfreq
        //conviene fare in un altro file e aggiungere offset e lunghezza del blocco (long, int)
        //File da dichiarare per leggere:
        //docindex -> ok
        //docids -> ok
        //tf -> ok
        //File da dichiarare per leggere e scrivere:
        //lexicon -> lettura ok
        //skipinfo ->ok
        RandomAccessFile inDocsFile = new RandomAccessFile(new File("docs/docids.txt"),"rw");
        FileChannel docChannel = inDocsFile.getChannel();
        RandomAccessFile inTfFile = new RandomAccessFile(new File("docs/tfs.txt"),"rw");
        FileChannel tfChannel = inTfFile.getChannel();
        RandomAccessFile inLexFile = new RandomAccessFile(new File("docs/lexicon.txt"),"rw");
        FileChannel lexChannel = inLexFile.getChannel();
        RandomAccessFile docIndexFile = new RandomAccessFile(new File("docs/docIndex.txt"),"rw");
        FileChannel docIndexChannel = docIndexFile.getChannel();
        RandomAccessFile skipInfoFile = new RandomAccessFile(new File("docs/skipInfo.txt"),"rw");
        FileChannel skipInfoChannel = skipInfoFile.getChannel();
        //output file for the updated lexicon with the skip info pointers and term upper bound
        RandomAccessFile outLexFile = new RandomAccessFile(new File("docs/lexiconTot.txt"),"rw");
        FileChannel outLexChannel = outLexFile.getChannel();
        Compressor c = new Compressor();
        //for each term we read the posting list, decompress it and compute the max score
        int totLen = 0;
        int entrySize = ConfigurationParameters.LEXICON_ENTRY_SIZE;
        long lexOffset = 0;
        long skipOffset = 0;
        long docOffset = 0; //offset of the docids list in the docids output file
        long tfOffset = 0; //offset of the tfs list in the tfs output file
        while(totLen<inLexFile.length()){
            int skipLen = 0;
            ByteBuffer readBuffer = ByteBuffer.allocate(entrySize);
            //we set the position in the files using the offsets
            lexChannel.position(lexOffset);
            lexChannel.read(readBuffer);
            readBuffer.position(0);
            //read first 22 bytes for the term
            ByteBuffer term = ByteBuffer.allocate(entrySize);
            readBuffer.get(term.array(), 0, 22);
            //read remaining bytes for the lexicon stats
            ByteBuffer val = ByteBuffer.allocate(entrySize-22);
            readBuffer.get(val.array(), 0, entrySize-22);
            //we use a method for reading the 36 bytes in a LexiconStats object
            LexiconStats l = new LexiconStats(val);
            //convert the bytes to the String
            String word = Text.decode(term.array());
            //replace null characters
            word = word.replaceAll("\0", "");
            //now we read the inverted files and compute the scores
            long offsetDoc = l.getOffsetDocid();
            long offsetTf = l.getOffsetTf();
            int docLen = l.getDocidsLen();
            int tfLen = l.getTfLen();
            docChannel.position(offsetDoc);
            ByteBuffer docids = ByteBuffer.allocate(docLen);
            docChannel.read(docids);

            List<Integer> decompressedDocids = c.variableByteDecode(docids.array());
            tfChannel.position(offsetTf);
            ByteBuffer tfs = ByteBuffer.allocate(tfLen);
            tfChannel.read(tfs);
            List<Integer> decompressedTfs = c.unaryDecode(tfs.array());
            double maxscore = 0.0;
            double tfidfMaxScore = 0.0;
            for(int i = 0; i < decompressedDocids.size(); i++){
                int tf = decompressedTfs.get(i);
                double idf = l.getIdf();
                int documentLength = Utils.getDocLen(docIndexChannel, decompressedDocids.get(i).toString());
                double score = Scorer.bm25Weight(tf, documentLength, idf);
                if(score>maxscore){
                    maxscore = score;
                }
                double scoreTfIdf = Scorer.tfidf(tf, documentLength, idf);
                if(scoreTfIdf>tfidfMaxScore){
                    tfidfMaxScore = scoreTfIdf;
                }
            }
            int nBlocks = (int) Math.floor(Math.sqrt(l.getdF()));
            int nDocids = 0;
            byte[] skips = new byte[0];
            while(nDocids < decompressedDocids.size()){
                int nBytes = 0;
                int tfBytes = 0;
                int i = nDocids;
                if(nDocids+nBlocks> decompressedDocids.size())
                    nDocids = decompressedDocids.size();
                else nDocids += nBlocks;
                int docid = decompressedDocids.get(nDocids-1);
                while(i <= nDocids-1){
                    nBytes += c.variableByteEncodeNumber(decompressedDocids.get(i)).length;
                    tfBytes += c.unaryEncode(decompressedTfs.get(i)).length;
                    //System.out.println("Bytes: " + nBytes);
                    i++;
                }
                //write in the skip info file the pair (endDocid,nBytes)
                byte[] endDocidBytes = ByteBuffer.allocate(4).putInt(docid).array();
                //System.out.println("End docid bytes: " + docid + " " + endDocidBytes.length);
                byte[] numBytes = ByteBuffer.allocate(4).putInt(nBytes).array();
                //System.out.println("Bytes docid: " + docid + " " + nBytes);
                byte[] numTfBytes = ByteBuffer.allocate(4).putInt(tfBytes).array();
                //System.out.println("Bytes tf: " + docid + " " + tfBytes);
                endDocidBytes = addByteArray(endDocidBytes,numBytes);
                endDocidBytes = addByteArray(endDocidBytes,numTfBytes);
                skipLen+=endDocidBytes.length;
                if(skips.length == 1){
                    skips = endDocidBytes;
                }
                else{
                    skips = addByteArray(skips, endDocidBytes);
                }
            }
            //write the skip blocks in the file
            ByteBuffer bufferSkips = ByteBuffer.allocate(skips.length);
            bufferSkips.put(skips);
            bufferSkips.flip();
            skipInfoChannel.write(bufferSkips);
            //write the new lexicon entry
            byte[] lexiconBytes = Utils.getBytesFromString(word);
            lexiconBytes = addByteArray(lexiconBytes, Utils.createLexiconEntry(l.getdF(), l.getCf(), docLen, tfLen, l.getOffsetDocid(), l.getOffsetTf(), l.getIdf(), maxscore, tfidfMaxScore, skipOffset, skipLen));
            //write lexicon entry to disk
            ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
            bufferLex.put(lexiconBytes);
            bufferLex.flip();
            outLexChannel.write(bufferLex);
            skipOffset+=skipLen; //update the offset on the skip info file
            lexOffset+=entrySize; //update the offset on the lexicon file
            totLen+=entrySize; //go to the next entry of the lexicon file
        }

    }

}




