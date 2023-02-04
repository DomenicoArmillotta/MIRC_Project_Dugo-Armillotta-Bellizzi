package indexing;

import compression.Compressor;
import fileManager.ConfigurationParameters;
import fileManager.FileOpener;
import invertedIndex.InvertedIndex;
import invertedIndex.LexiconEntry;
import invertedIndex.LexiconStats;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;
import preprocessing.Preprocessor;
import queryProcessing.MaxScore;
import utility.Utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static utility.Utils.addByteArray;

public class SPIMI {
    private InvertedIndex invertedIndex;
    private int docid = 0;
    private static double totalLength = 0;
    private static double numDocs = 0;
    private FileChannel docIndexChannel;
    private Preprocessor preprocessor;

    public void spimiInvertBlock(String readPath, boolean mode) throws IOException {
        preprocessor = new Preprocessor();
        InputStream stream = FileOpener.extractFromZip(readPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        RandomAccessFile docIndexFile = new RandomAccessFile(new File("docs/docIndex.txt"), "rw");
        docIndexChannel = docIndexFile.getChannel();
        int indexBlock = 0;
        try {
            String line="";
            //create chunk of data , splitting in n different block
            while (line!=null){
                System.out.println("Block: " + indexBlock + " " + docid + " " + line);
                //instantiate a new Inverted Index and Lexicon per block
                invertedIndex = new InvertedIndex(indexBlock);
                while ((line = reader.readLine())!=null){
                    spimiInvert(line,mode);
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
            reader.close();
            stream.close();
        }
        //compute the average document length
        Utils.createDocumentStatistics(totalLength, numDocs);
        mergeBlocks(indexBlock);
    }

    public void spimiInvertBlockUncompressed(String readPath, boolean mode) throws IOException {
        File inputFile = new File(readPath);
        LineIterator it = FileUtils.lineIterator(inputFile, "UTF-8");
        RandomAccessFile docIndexFile = new RandomAccessFile(new File("docs/docIndex.txt"), "rw");
        docIndexChannel = docIndexFile.getChannel();
        int indexBlock = 0;
        //int cont = 0;
        try {
            //create chunk of data , splitting in n different block
            while (it.hasNext()){
                System.out.println("Block: " + indexBlock);
                //instantiate a new Inverted Index and Lexicon per block
                invertedIndex = new InvertedIndex(indexBlock);
                while (it.hasNext()){
                    String line = it.nextLine();
                    spimiInvert(line,mode);
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
        //compute the average document length
        Utils.createDocumentStatistics(totalLength, numDocs);
        mergeBlocks(indexBlock);
    }

    public void spimiInvert(String doc, boolean mode) throws IOException {
        String[] parts = doc.split("\t");
        String docno = parts[0];
        String corpus = parts[1];
        List<String> tokenizedDoc;
        if(mode){
            tokenizedDoc = preprocessor.preprocessDocument(corpus);
        }
        else{
            tokenizedDoc = preprocessor.preprocessDocumentUnfiltered(corpus);
        }
        docid++;
        int cont = 0;
        //read the terms and generate postings
        for (String term : tokenizedDoc) {
            invertedIndex.addPosting(term, docid, 1);
            cont++;
        }
        totalLength+=cont;
        numDocs++;
        //write the document index entry
        Text key = new Text(docno);
        byte[] docIndexBytes;
        if (key.getLength() >= 9) {
            Text truncKey = new Text(docno.substring(0, 8));
            docIndexBytes = truncKey.getBytes();
        } else { //we allocate 10 bytes for the Text object, which is a string of 20 chars
            docIndexBytes = ByteBuffer.allocate(10).put(key.getBytes()).array();
        }
        //take the docid
        byte[] docidBytes = ByteBuffer.allocate(4).putInt(docid).array();
        //take the document frequency
        byte[] dfBytes = ByteBuffer.allocate(4).putInt(cont).array();
        //concatenate all the byte arrays in order
        docIndexBytes = addByteArray(docIndexBytes, docidBytes);
        docIndexBytes = addByteArray(docIndexBytes, dfBytes);
        //write lexicon entry to disk
        ByteBuffer bufferDoc = ByteBuffer.allocate(docIndexBytes.length);
        bufferDoc.put(docIndexBytes);
        bufferDoc.flip();
        docIndexChannel.write(bufferDoc);
        bufferDoc.clear();
    }

    public void mergeBlocks(int n) throws IOException {
        long start = System.currentTimeMillis();
        System.out.println("START MERGING");
        //declare files to read
        List<String> lexPaths = new ArrayList<>();
        List<String> docPaths = new ArrayList<>();
        List<String> tfPaths = new ArrayList<>();
        for(int i = 0; i <= n; i++){
            lexPaths.add("docs/lexicon_"+i+".txt");
            docPaths.add("docs/docids_"+i+".txt");
            tfPaths.add("docs/tfs_"+i+".txt");
        }
        //take the entry size of the lexicon from the configuration parameters
        int entrySize = ConfigurationParameters.LEXICON_ENTRY_SIZE;
        String currLex1 = lexPaths.get(0);
        String currDocs1 = docPaths.get(0);
        String currTf1 = tfPaths.get(0);
        double N = numDocs; //take the total number of documents in the collection
        //in the case of multiple block to merge
        int nFile = 1;
        while(nFile < n){
            System.out.println("HERE " + nFile);
            String currLex2 = lexPaths.get(nFile);
            String currDocs2 = docPaths.get(nFile);
            String currTf2 =tfPaths.get(nFile);
            //when total_size is equal to the sum of the lengths of the files, we have finished to read
            long totalSize1 = 0; //we need to keep track of the total length of the file(s) to merge;
            long totalSize2 = 0;
            //output files
            String docPath = "docs/tempD"+nFile+".txt";
            String tfPath = "docs/tempT"+nFile+".txt";
            String lexPath = "docs/tempL"+nFile+".txt";
            System.out.println("Input files: " + currLex1 + " " + currLex2);
            System.out.println("Output file: " +docPath + ", " + tfPath);
            RandomAccessFile outDocsFile = new RandomAccessFile(new File(docPath),"rw");
            FileChannel tempDocChannel = outDocsFile.getChannel();
            RandomAccessFile outTfFile = new RandomAccessFile(new File(tfPath),"rw");
            FileChannel tempTfChannel = outTfFile.getChannel();
            RandomAccessFile outFile = new RandomAccessFile(new File(lexPath),"rw");
            FileChannel tempChannel = outFile.getChannel();
            //declare input files
            RandomAccessFile doc1File = new RandomAccessFile(new File(currDocs1),"rw");
            FileChannel doc1Channel = doc1File.getChannel();
            RandomAccessFile tf1File = new RandomAccessFile(new File(currTf1),"rw");
            FileChannel tf1Channel = tf1File.getChannel();
            RandomAccessFile lex1File = new RandomAccessFile(new File(currLex1),"rw");
            FileChannel lex1Channel = lex1File.getChannel();
            RandomAccessFile doc2File = new RandomAccessFile(new File(currDocs2),"rw");
            FileChannel doc2Channel = doc2File.getChannel();
            RandomAccessFile tf2File = new RandomAccessFile(new File(currTf2),"rw");
            FileChannel tf2Channel = tf2File.getChannel();
            RandomAccessFile lex2File = new RandomAccessFile(new File(currLex2),"rw");
            FileChannel lex2Channel = lex2File.getChannel();
            //for reading we use the pointers with the position method, so we need the offsets of the two files
            //offsets for reading the two files
            long offset1 = 0;
            long offset2 = 0;
            long docOffset = 0; //offset of the docids list in the docids output file
            long tfOffset = 0; //offset of the tfs list in the tfs output file
            while(totalSize1 +totalSize2 < lex1Channel.size() + lex2Channel.size()){
                LexiconEntry entry1 = Utils.getLexiconEntry(lex1Channel, offset1);
                LexiconEntry entry2 = Utils.getLexiconEntry(lex2Channel, offset2);
                String word1 = entry1.getTerm();
                String word2 = entry2.getTerm();
                LexiconStats l1 = entry1.getLexiconStats();
                LexiconStats l2 = entry2.getLexiconStats();
                //compare terms to see what to merge in the result
                //check if the first term is greater than the second or if the second is greater than the other
                //if they are equal we merge them in the LexiconStats
                int docLen = 0;
                int tfLen = 0;
                String word = "";
                double idf = 0;
                int df = 0;
                long cF = 0;
                //we also have to check if the other file is finished, in that case the compare doesn't work
                if(word1.compareTo(word2) < 0 || totalSize2==lex2Channel.size()){
                    word = word1;
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
                    //idf value
                    long nn = l1.getdF(); // number of documents that contain the term t among the data set
                    idf = Math.log((N/nn));
                    docLen = l1.getDocidsLen();
                    tfLen = l1.getTfLen();
                    df = l1.getdF();
                    cF = l1.getCf();
                    offset1+= entrySize;
                    totalSize1+=entrySize;
                }
                else if(word2.compareTo(word1) < 0 || totalSize1 == lex1Channel.size()){
                    word = word2;
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
                    //idf value
                    long nn = l2.getdF(); // number of documents that contain the term t among the data set
                    idf = Math.log((N/nn));
                    docLen = l2.getDocidsLen();
                    tfLen = l2.getTfLen();
                    df = l2.getdF();
                    cF = l2.getCf();
                    offset2 += entrySize;
                    totalSize2+=entrySize;
                }
                else if (word1.compareTo(word2) == 0){
                    word = word1;
                    //MERGE: we merge the docids, the tfs in the files; we merge the dF, cF, docidslen and tfslen
                    //in the new merged files we need to write the term and the new lexicon stats with the
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
                    docLen = l1.getDocidsLen()+l2.getDocidsLen();
                    tfLen = l1.getTfLen()+l2.getTfLen();
                    //idf value
                    long nn = l1.getdF()+l2.getdF(); // number of documents that contain the term t among the data set
                    idf = Math.log((N/nn));
                    df = l1.getdF()+l2.getdF();
                    cF = l1.getCf()+l2.getCf();
                    offset1 += entrySize;
                    offset2 += entrySize;
                    totalSize1+=entrySize;
                    totalSize2+=entrySize;
                }
                byte[] lexiconBytes = Utils.getBytesFromString(word);
                lexiconBytes = addByteArray(lexiconBytes, Utils.createLexiconEntry(df, cF, docLen,tfLen, docOffset, tfOffset, idf, 0.0, 0.0, 0, 0));
                //write lexicon entry to disk
                ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
                bufferLex.put(lexiconBytes);
                bufferLex.flip();
                tempChannel.write(bufferLex); //write in lexicon file
                //update offsets
                docOffset+=docLen;
                tfOffset+=tfLen;
            }
            //update file paths
            currLex1 = lexPath;
            currDocs1 = docPath;
            currTf1 = tfPath;
            nFile++; //increment number of files read
        }
        long end = System.currentTimeMillis();
        System.out.println("END MERGING");
        long time = ((end-start)/1000)/60;
        System.out.println("Time for merging the files: " + time + " minutes");
        //Writing the output files
        createFinalIndex(currLex1, currDocs1, currTf1);
    }

    public void createFinalIndex(String lexPath, String docsPath, String tfPath) throws IOException {
        //declare output file and channels
        File lexFile = new File("docs/lexicon.txt");
        File docFile = new File("docs/docids.txt");
        File tfFile = new File("docs/tfs.txt");
        RandomAccessFile streamLex = new RandomAccessFile(lexFile, "rw");
        RandomAccessFile streamDocs = new RandomAccessFile(docFile, "rw");
        RandomAccessFile streamTf = new RandomAccessFile(tfFile, "rw");
        RandomAccessFile skipInfoFile = new RandomAccessFile(new File("docs/skipInfo.txt"),"rw");
        FileChannel lexChannel = streamLex.getChannel();
        FileChannel docChannel = streamDocs.getChannel();
        FileChannel tfChannel = streamTf.getChannel();
        FileChannel skipInfoChannel = skipInfoFile.getChannel();
        //declare the input channels
        RandomAccessFile inputDocFile = new RandomAccessFile(new File(docsPath),"rw");
        FileChannel inputDocChannel = inputDocFile.getChannel();
        RandomAccessFile inputTfFile = new RandomAccessFile(new File(tfPath),"rw");
        FileChannel inputTfChannel = inputTfFile.getChannel();
        RandomAccessFile inputLexFile = new RandomAccessFile(new File(lexPath),"rw");
        FileChannel inputLexChannel = inputLexFile.getChannel();
        Compressor compressor = new Compressor();
        DocumentIndex documentIndex = new DocumentIndex();
        HashMap<Integer, Integer> docIndex = documentIndex.getDocIndex();
        int totLen = 0; // to keep track of the total length of the file
        int entrySize = ConfigurationParameters.LEXICON_ENTRY_SIZE;
        int keySize = ConfigurationParameters.LEXICON_KEY_SIZE;
        long lexOffset = 0; //ofset of the lexicon entry in the lexicon output file
        long skipOffset = 0; //offset of the skip info entry in the skip info file
        long docOffset = 0; //offset of the docids list in the docids output file
        long tfOffset = 0; //offset of the tfs list in the tfs output file
        long totDocIdsOld = 0; //to keep track of the length of the uncompressed docIds file
        long totDocIdsNew = 0; //to keep track of the length of the compressed docIds file
        long totTfsOld = 0; //to keep track of the length of the uncompressed tfs file
        long totTfsNew = 0; //to keep track of the length of the compressed tfs file
        double N = ConfigurationParameters.getNumberOfDocuments(); //take the total number of documents in the collection
        while(totLen<inputLexFile.length()){
            int skipLen = 0;
            LexiconEntry entry = Utils.getLexiconEntry(inputLexChannel, lexOffset);
            String word = entry.getTerm();
            LexiconStats lexiconStats = entry.getLexiconStats();
            //now we read the inverted files and compute the scores
            long offsetDoc = lexiconStats.getOffsetDocid();
            long offsetTf = lexiconStats.getOffsetTf();
            int docLen = lexiconStats.getDocidsLen();
            int tfLen = lexiconStats.getTfLen();
            inputDocChannel.position(offsetDoc);
            ByteBuffer docids = ByteBuffer.allocate(docLen);
            inputDocChannel.read(docids);
            inputTfChannel.position(offsetTf);
            ByteBuffer tfs = ByteBuffer.allocate(tfLen);
            inputTfChannel.read(tfs);
            int offDoc = 0;
            int offTf = 0;
            int newDocLen = 0;
            int newTfLen = 0;
            double maxscore = 0.0;
            double tfidfMaxScore = 0.0;
            int nBlocks = (int) Math.floor(Math.sqrt(lexiconStats.getdF())); //number of posting for each block
            int listSize = docLen/4;
            int nBytes = 0;
            int tfBytes = 0;
            double idf = Math.log(N/lexiconStats.getdF());
            for(int i = 0; i < listSize; i++){
                docids.position(offDoc);
                tfs.position(offTf);
                int docId = docids.getInt();
                int tf = tfs.getInt();
                int documentLength = docIndex.get(docId);
                maxscore = MaxScore.getMaxScoreBM25(maxscore,tf,documentLength,idf);
                tfidfMaxScore = MaxScore.getMaxScoreTFIDF(tfidfMaxScore,tf,idf);
                //write posting list into compressed file : for each posting compress and write on appropriate file
                //compress the docid with variable byte
                byte[] compressedDocs = compressor.variableByteEncodeNumber(docId);
                ByteBuffer bufferValue = ByteBuffer.allocate(compressedDocs.length);
                bufferValue.put(compressedDocs);
                bufferValue.flip();
                docChannel.write(bufferValue);
                //compress the TermFreq with unary
                byte[] compressedTF = compressor.unaryEncode(tf); //compress the term frequency with unary
                ByteBuffer bufferFreq = ByteBuffer.allocate(compressedTF.length);
                bufferFreq.put(compressedTF);
                bufferFreq.flip();
                tfChannel.write(bufferFreq);
                nBytes += compressedDocs.length;
                tfBytes += compressedTF.length;
                newDocLen+=compressedDocs.length;
                newTfLen+=compressedTF.length;
                if((i+1)%nBlocks == 0 || i+1 == listSize){ //we reached the end of a block
                    byte[] skipBytes = Utils.createSkipInfoBlock(docId, nBytes, tfBytes);
                    skipLen+=skipBytes.length;
                    //write the skip blocks in the file
                    ByteBuffer bufferSkips = ByteBuffer.allocate(skipBytes.length);
                    bufferSkips.put(skipBytes);
                    bufferSkips.flip();
                    skipInfoChannel.write(bufferSkips);
                    nBytes = 0;
                    tfBytes = 0;
                }
                offDoc+=4;
                offTf+=4;
            }
            //update lengths of the files
            totDocIdsOld+=offDoc;
            totDocIdsNew+=newDocLen;
            totTfsOld+=offTf;
            totTfsNew+=newTfLen;
            //System.out.println(word + ": " + idf + " " + maxscore + " " + tfidfMaxScore);
            //write the new lexicon entry
            byte[] lexiconBytes = Utils.getBytesFromString(word);
            lexiconBytes = addByteArray(lexiconBytes, Utils.createLexiconEntry(lexiconStats.getdF(), lexiconStats.getCf(), newDocLen, newTfLen, docOffset, tfOffset, idf, maxscore, tfidfMaxScore, skipOffset, skipLen));
            //write lexicon entry to disk
            ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
            bufferLex.put(lexiconBytes);
            bufferLex.flip();
            lexChannel.write(bufferLex);
            skipOffset+=skipLen; //update the offset on the skip info file
            lexOffset+=entrySize; //update the offset on the lexicon file
            docOffset+=newDocLen; //update the offset on the docIds file
            tfOffset+=newTfLen; //update the offset on the tf file
            totLen+=entrySize; //go to the next entry of the lexicon file
        }
        System.out.println("Total bytes of the uncompressed docIds file: " + totDocIdsOld);
        System.out.println("Total bytes of the compressed docIds file: " + totDocIdsNew);
        System.out.println("Total bytes of the uncompressed term frequencies file: " + totTfsOld);
        System.out.println("Total bytes of the compressed term frequencies file: " + totTfsNew);
    }
}




