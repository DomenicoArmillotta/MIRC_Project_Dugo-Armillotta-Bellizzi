package indexing;

import compression.Compressor;
import fileManager.CollectionParser;
import fileManager.ConfigurationParameters;
import invertedIndex.LexiconStats;
import invertedIndex.Posting;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.io.Text;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import preprocessing.PreprocessDoc;
import queryProcessing.Scorer;
import utility.Utils;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static utility.Utils.addByteArray;

public class SPIMI_InvertTest extends TestCase {


    public void testSpimi() throws IOException {
        SPIMI s = new SPIMI();
        s.spimiInvertBlockMapped("docs/collection_test3.tsv");
    }

    public void testMaxScores() throws IOException {
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
            //read first 22 bytes for the term
            readBuffer.position(0); //aggiungere anche nel merging
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
            //System.out.println("Docids: "  + decompressedDocids);
            //System.out.println("Tfs: " + decompressedTfs);
            //System.out.println(word + " " + l.getdF() + " " + l.getCf() + " " + l.getDocidsLen() + " " + l.getTfLen() + " " + l.getIdf());

            double maxscore = 0.0;
            for(int i = 0; i < decompressedDocids.size(); i++){
                int tf = decompressedTfs.get(i);
                double idf = l.getIdf();
                int documentLength = Utils.getDocLen(docIndexChannel, decompressedDocids.get(i).toString());
                double score = Scorer.bm25Weight(tf, documentLength, idf);
                if(score>maxscore){
                    maxscore = score;
                }
            }
            System.out.println(word + ": " + maxscore);
            int nBlocks = (int) Math.floor(Math.sqrt(l.getdF()));
            int nDocids = 0;
            int nBytes = 0;
            byte[] skips = new byte[0];
            while(nDocids < decompressedDocids.size()){
                int i = nDocids;
                if(nDocids+nBlocks> decompressedDocids.size())
                    nDocids = decompressedDocids.size();
                else nDocids += nBlocks;
                int docid = decompressedDocids.get(nDocids-1);
                while(i <= nDocids-1){
                    nBytes += c.variableByteEncodeNumber(decompressedDocids.get(i)).length;
                    i++;
                }
                //write in the skip info file the pair (endDocid,nBytes)
                byte[] endDocidBytes = ByteBuffer.allocate(4).putInt(docid).array();
                byte[] numBytes = ByteBuffer.allocate(4).putInt(nBytes).array();
                endDocidBytes = addByteArray(endDocidBytes,numBytes);
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
            //take the document frequency
            byte[] dfBytes = ByteBuffer.allocate(4).putInt(l.getdF()).array();
            //take the collection frequency
            byte[] cfBytes = ByteBuffer.allocate(8).putLong(l.getCf()).array();
            //take list dim for both docids and tfs
            byte[] docBytes = ByteBuffer.allocate(4).putInt(docLen).array();
            byte[] tfBytes = ByteBuffer.allocate(4).putInt(tfLen).array();
            //take the offset of docids
            byte[] offsetDocBytes = ByteBuffer.allocate(8).putLong(l.getOffsetDocid()).array();
            //take the offset of tfs
            byte[] offsetTfBytes = ByteBuffer.allocate(8).putLong(l.getOffsetDocid()).array();
            byte[] idfBytes = ByteBuffer.allocate(8).putDouble(l.getIdf()).array();
            byte[] tupBytes = ByteBuffer.allocate(8).putDouble(maxscore).array();
            byte[] offsetSkipBytes = ByteBuffer.allocate(8).putLong(skipOffset).array();
            byte[] skipBytes = ByteBuffer.allocate(4).putInt(skipLen).array();
            //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
            lexiconBytes = addByteArray(lexiconBytes,dfBytes);
            lexiconBytes = addByteArray(lexiconBytes,cfBytes);
            lexiconBytes = addByteArray(lexiconBytes,docBytes);
            lexiconBytes = addByteArray(lexiconBytes,tfBytes);
            lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
            lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
            lexiconBytes = addByteArray(lexiconBytes,idfBytes);
            lexiconBytes = addByteArray(lexiconBytes,tupBytes);
            lexiconBytes = addByteArray(lexiconBytes,offsetSkipBytes);
            lexiconBytes = addByteArray(lexiconBytes,skipBytes);
            //write lexicon entry to disk
            ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
            bufferLex.put(lexiconBytes);
            bufferLex.flip();
            outLexChannel.write(bufferLex);
            skipOffset+=skipLen; //update the offset on the skip info file
            lexOffset+=entrySize; //update the offset on the lexicon file
            totLen+=entrySize; //go to the next entry of the lexicon file
            System.out.println(skipOffset);
        }
    }

    public int getDocLen(FileChannel channel, String key) throws IOException {
        int docLen = 0;
        int entrySize = ConfigurationParameters.DOC_INDEX_ENTRY_SIZE;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
        int lowerBound = 0;
        int upperBound = (int) channel.size()-entrySize;
        /*while(lowerBound!=channel.size()){
            buffer.position(lowerBound);
            ByteBuffer ba = ByteBuffer.allocate(10);
            buffer.get(ba.array(), 0, 10);
            if(ba.hasArray()) {
                byte[] term = new byte[22];
                term = ba.array();
                String value = Text.decode(term);
                value = value.replaceAll("\0", "");
                System.out.println(value + " " + lowerBound);
            }
            lowerBound+=14;
        }*/
        while (lowerBound <= upperBound) {
            int midpoint = (lowerBound + upperBound) / 2;
            if(midpoint%entrySize!=0){
                midpoint += midpoint%entrySize;
            }
            buffer.position(midpoint);
            ByteBuffer ba = ByteBuffer.allocate(10);
            buffer.get(ba.array(), 0, 10);
            String value = Text.decode(ba.array());
            value = value.replaceAll("\0", "");
            if (value.equals(key)) {
                System.out.println("Found key " + key + " at position " + midpoint);
                ByteBuffer bf1 = ByteBuffer.allocate(4);
                buffer.get(bf1.array(), 0, 4);
                docLen = bf1.getInt();
                break;
            } else if (Integer.parseInt(key) - Integer.parseInt(value) < 0) {
                upperBound = midpoint - entrySize;
            } else {
                lowerBound = midpoint + entrySize;
            }
        }
        return docLen;
    }
    public void testParser() throws IOException {
        CollectionParser cp = new CollectionParser();
        cp.parseFile("docs/collection_test3.tsv");
        RandomAccessFile outFile = new RandomAccessFile(new File("docs/parameters.txt"), "rw");
        RandomAccessFile docIndexFile = new RandomAccessFile(new File("docs/docIndex.txt"), "rw");
        FileChannel docIndexChannel = docIndexFile.getChannel();
        FileChannel outChannel = outFile.getChannel();
        int pos = 0;
        outChannel.position(0);
        ByteBuffer avgLen = ByteBuffer.allocate(8);
        ByteBuffer totLen = ByteBuffer.allocate(8);
        ByteBuffer numDocs = ByteBuffer.allocate(8);
        outChannel.read(avgLen);
        pos+=8;
        outChannel.position(pos);
        outChannel.read(totLen);
        avgLen.flip();
        totLen.flip();
        pos+=8;
        outChannel.position(pos);
        outChannel.read(numDocs);
        outChannel.read(numDocs);
        outChannel.position(pos);
        numDocs.flip();
        System.out.println(avgLen.getDouble() + " " + totLen.getDouble() + " " + numDocs.getDouble());
        System.out.println(getDocLen(docIndexChannel, "65"));
    }

    public void testDocSearch() throws IOException {
        RandomAccessFile docIndexFile = new RandomAccessFile(new File("docs/docIndex.txt"), "rw");
        FileChannel docIndexChannel = docIndexFile.getChannel();
        System.out.println(getDocLen(docIndexChannel, "65"));
        System.out.println(getDocLen(docIndexChannel, "234"));
        System.out.println(getDocLen(docIndexChannel, "876"));
        System.out.println(getDocLen(docIndexChannel, "4092"));

    }

    public void testMapDbList(){
        //use DBMaker to create a DB object stored on disk
        //provide output location of list
        DB db = DBMaker.fileDB("docs/testMapDBList.db").make();

        //use the DB object to create the "myList" ArrayList
        //set the specific serializer for better performance
        List<String> list = db.indexTreeList("myList", Serializer.STRING).createOrOpen();

        //populate list
        for (int i = 0; i < 1000; i++) {
            list.add("item" + i);
        }

        //persist changes on disk
        db.commit();

        //close to protect from data corruption
        db.close();

        //use DBMaker to create a DB object of List stored on disk
        //provide location
        DB readDb = DBMaker.fileDB("docs/testMapDBList.db").make();

        //use the DB object to open the "myList" ArrayList
        List<String> readList = readDb.indexTreeList("myList", Serializer.STRING).createOrOpen();

        //read from list
        for (int i = 0; i < 1000; i++) {
            System.out.println(readList.get(i));
        }

        //close to protect from data corruption
        db.close();
    }

    public void testSpimiMapped() throws IOException {
        SPIMI spimi = new SPIMI();
        String path = "docs/collection_test.tsv";
        spimi.spimiInvertBlockMapped(path);
        DB readDb = DBMaker.fileDB("docs/index0.db").make();
        DB readDoc = DBMaker.fileDB("docs/testDB.db").make();

        HTreeMap<String, List<Posting>> readInvertedIndex = readDb.hashMap("invertedIndex0")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        List<String> readList = readDb.indexTreeList("lexicon0", Serializer.STRING).createOrOpen();
        /*Set<String> readLex = readDb.hashSet("lexicon0")
                .serializer(Serializer.STRING)
                .createOrOpen();*/
        /*HTreeMap<String, Integer> readLexicon = readDb.hashMap("lexicon0")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();*/
        HTreeMap<String, Integer> readDocIndex = readDoc.hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();

        //System.out.println(readDocIndex.get("2"));
        //read from list
        /*for (int i = 0; i < readList.size(); i++) {
            System.out.println(readList.get(i));
        }*/
        System.out.println(readInvertedIndex.keySet().toArray()[0]);
        readInvertedIndex.entrySet().stream().sorted(Map.Entry.<String, List<Posting>>comparingByKey()).forEach(System.out::println);
        //readInvertedIndex.entrySet().stream().forEach(System.out::println);
        /*System.out.println(readInvertedIndex.get("bile"));
        System.out.println(readInvertedIndex.get("area"));
        System.out.println(readDocIndex.get("35"));
        System.out.println(readDocIndex.get("99"));*/
    }


        /*DB readDb = DBMaker.fileDB("docs/testPointers.db").make();
        HTreeMap<String, Integer> readLexicon = readDb.hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
        HTreeMap<String, Integer> readDocIndex = readDb.hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
        List<Posting> list = (List<Posting>) readDb.indexTreeList("bile", Serializer.JAVA).createOrOpen();
        System.out.println(list);
        List<Posting> list2 = (List<Posting>) readDb.indexTreeList("abdomin", Serializer.JAVA).createOrOpen();
        System.out.println(list2);
        List<Posting> list3 = (List<Posting>) readDb.indexTreeList("credit", Serializer.JAVA).createOrOpen();
        System.out.println(list3);
        //close to protect from data corruption
        db.close();*/

    public void testMapDb() throws IOException {
        //use DBMaker to create a DB object stored on disk
        //provide output location of list
        String path = "docs/collection_test.tsv";
        DB db = DBMaker.fileDB("docs/testMapDBIndex.db").make();
        //List<Posting> list = db.indexTreeList("myList", Serializer.JAVA).create();
        //HTreeMap myMap = db.hashMap("invertedIndex").createOrOpen();
        HTreeMap<String, List<Posting>> invertedIndex = db
                .hashMap("invertedIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .create();
        HTreeMap<String, Integer> lexicon = db
                .hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .create();
        HTreeMap<String, Integer> documentIndex = db
                .hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .create();
        //use the DB object to create the "myList" ArrayList
        //set the specific serializer for better performance
        /*List<String> list = db.indexTreeList("myList", Serializer.STRING).createOrOpen();

        //populate list
        for (int i = 0; i < 1000; i++) {
            list.add("item" + i);
        }*/
        /*int docid = 0;
        PreprocessDoc preprocessDoc = new PreprocessDoc();
        File input_file = new File(path);
        LineIterator it = FileUtils.lineIterator(input_file, "UTF-8");
        try{
            while (it.hasNext()) {
                String doc = it.nextLine();
                int cont = 0;
                String[] parts = doc.split("\t");
                String docno = parts[0];
                String doc_corpus = parts[1];
                List<String> pro_doc = preprocessDoc.preprocess_doc_optimized(doc_corpus);
                //read the terms and generate postings
                //write postings
                for (String term : pro_doc) {
                    lexicon.put(term, cont);
                    //addPosting(invertedIndex, term, doc_id, 1);
                    //Posting p = new Posting(docid, 1);
                    if(invertedIndex.get(term) == null){
                        List<Posting> l = new ArrayList<>();
                        l.add(new Posting(docid, 1));
                        invertedIndex.put(term,l);
                    }
                    else {
                        List<Posting> l = invertedIndex.get(term);
                        boolean change = false;
                        for (Posting posting : l) {
                            if (posting.getDocumentId() == docid) {
                                posting.addOccurrence();
                                invertedIndex.put(term, l);
                                change = true;
                                //return;
                            }
                        }
                        if(!change) {
                            l.add(new Posting(docid, 1));
                            invertedIndex.put(term, l);
                        }
                    }
                    //invertedIndex.get(term).add();
                    //index.addToDict(term);
                    //index.addPosting(term, doc_id, 1, cont);
                    cont++;
                }
                documentIndex.put(docno, cont);
                docid++;

            }
        }
        catch(IOException e){
            db.close();
        }

        //persist changes on disk
        db.commit();

        //close to protect from data corruption
        db.close();

        DB readDb = DBMaker.fileDB("docs/testMapDBIndex.db").make();

        HTreeMap<String, List<Posting>> readInvertedIndex = readDb.hashMap("invertedIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        HTreeMap<String, Integer> readLexicon = readDb.hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
        HTreeMap<String, Integer> readDocIndex = readDb.hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();

        //System.out.println(readDocIndex.get("2"));
        System.out.println(readInvertedIndex.get("bile"));
        System.out.println(readInvertedIndex.get("area"));

        //read from list
        for (int i = 0; i < 1000; i++) {
            System.out.println(readList.get(i));
        }
        */
        /*for(int i = 0; i < readDocIndex.size(); i++){
            System.out.println(readDocIndex.get(i));
        }*/

        //close to protect from data corruption
        //db.close();

        //use DBMaker to create a DB object of List stored on disk
        //provide location
        /*db = DBMaker.fileDB("docs/testMapDB.db").make();

        //use the DB object to open the "myList" ArrayList
        List<String> readList = db.indexTreeList("myList", Serializer.STRING).createOrOpen();

        //read from list
        for (int i = 0; i < 1000; i++) {
            System.out.println(readList.get(i));
        }

        //close to protect from data corruption
        db.close();*/
    }
/*
    public void testStringCompressionVBOld() throws IOException {
        Compressor compressor = new Compressor();
        String bitString = compressor.variableByte(60000);
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        File file = new File("docs/prova.bin");

        //write binary
        try (RandomAccessFile stream = new RandomAccessFile(file, "rw");
             FileChannel channel = stream.getChannel())
        {
            byte[] ba0 = new BigInteger(compressor.variableByte(0), 2).toByteArray();
            System.out.println(ba0.length);
            ByteBuffer bufferValue0 = ByteBuffer.allocate(ba0.length);
            //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
            //bufferValue2.put(System.getProperty("line.separator").getBytes());
            bufferValue0.put(ba0);
            bufferValue0.flip();
            channel.write(bufferValue0);
            System.out.println("WRITE: 0");
            byte[] ba4 = new BigInteger(compressor.variableByte(67822), 2).toByteArray();
            System.out.println(ba4.length);
            ByteBuffer bufferValue4 = ByteBuffer.allocate(ba4.length);
            bufferValue4.put(ba4);
            bufferValue4.flip();
            channel.write(bufferValue4);
            System.out.println("WRITE: 67822");
            byte[] ba2 = new BigInteger(compressor.variableByte(9), 2).toByteArray();
            System.out.println(ba2.length);
            ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length);
            //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
            //bufferValue2.put(System.getProperty("line.separator").getBytes());
            bufferValue2.put(ba2);
            bufferValue2.flip();
            channel.write(bufferValue2);
            System.out.println("WRITE: 9");
            ByteBuffer bufferValue = ByteBuffer.allocate(ba.length);
            bufferValue.put(ba);
            bufferValue.flip();
            channel.write(bufferValue);
            System.out.println("WRITE: 60000");
            int i = 0;
            while (i<5){
                byte[] ba1 = new BigInteger(compressor.variableByte(5), 2).toByteArray();
                System.out.println(ba1.length);
                ByteBuffer bufferValue1 = ByteBuffer.allocate(ba1.length);
                //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
                //bufferValue2.put(System.getProperty("line.separator").getBytes());
                bufferValue1.put(ba1);
                bufferValue1.flip();
                channel.write(bufferValue1);
                System.out.println("WRITE: 5");
                i++;
            }*/
            /*
            byte[] ba3 = new BigInteger(compressor.variableByteNew(67822), 2).toByteArray();
            ByteBuffer bufferValue2 = ByteBuffer.allocate(ba3.length);
            //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
            //bufferValue2.put(System.getProperty("line.separator").getBytes());
            bufferValue2.put(ba3);
            bufferValue2.flip();
            channel.write(bufferValue2);
            byte[] ba4 = new BigInteger(compressor.variableByteNew(1357), 2).toByteArray();
            ByteBuffer bufferValue4 = ByteBuffer.allocate(ba4.length);
            //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
            //bufferValue2.put(System.getProperty("line.separator").getBytes());
            bufferValue4.put(ba4);
            bufferValue4.flip();
            channel.write(bufferValue4);*/
            /*
            byte[] ba6 = new BigInteger(compressor.variableByte(10), 2).toByteArray();
            System.out.println(ba6.length);
            ByteBuffer bufferValue6 = ByteBuffer.allocate(ba6.length);
            //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
            //bufferValue2.put(System.getProperty("line.separator").getBytes());
            bufferValue6.put(ba6);
            bufferValue6.flip();
            channel.write(bufferValue6);
            System.out.println("WRITE: 10");
            byte[] ba5 = new BigInteger(compressor.variableByte(1357), 2).toByteArray();
            System.out.println(ba5.length);
            ByteBuffer bufferValue5 = ByteBuffer.allocate(ba5.length);
            //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
            //bufferValue2.put(System.getProperty("line.separator").getBytes());
            bufferValue5.put(ba5);
            bufferValue5.flip();
            channel.write(bufferValue5);
            System.out.println("WRITE: 1357");
            ByteBuffer bufferEnd = ByteBuffer.allocate("\n\r".getBytes().length);
            bufferEnd.put("\n\r".getBytes());
            bufferEnd.flip();
            channel.write(bufferEnd);
            System.out.println("Successfully written data to the file");
            stream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        //read
        // open filechannel
        String path = "docs/prova.bin";
        RandomAccessFile fileinput = new RandomAccessFile(path, "r");;
        FileChannel channel = fileinput.getChannel();
        //set the buffer size
        int bufferSize = 1;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        String prev = "";
        int nextValue = 0;
        // read the data from filechannel
        while (channel.read(buffer) != -1) {
            byte[] input = (buffer.array());
            BigInteger one = new BigInteger(input);
            if (one.compareTo(BigInteger.ZERO) < 0)
                one = one.add(BigInteger.ONE.shiftLeft(8));
            buffer.clear();
            //String str = String.format("%8s", one.toString(2)).replace(' ', '0');
            //System.out.println(str);
            String strResult = one.toString(2);
            //strResult = byteFormat(strResult);
            //System.out.println(strResult);
            if(strResult.equals("1010") && nextValue>10 && prev.equals("")){
                //prev = strResult;
                System.out.println("end line!");
                break;
            }*/
            /*if(strResult.equals("1101") && prev.equals("1010")) {
                System.out.println("new line! " + strResult);
                break;
            }*/
            /*if(strResult.length() == 8 || (strResult.equals("0") && nextValue!=0)){
                //System.out.println("not zero: "+ strResult);
                prev+=strResult;
            }
            else{
                if(prev!=""){
                    //System.out.println("previous: " + prev);
                    prev+=strResult;
                    //System.out.println("ending string: " + strResult);
                    //System.out.println("updated: " + prev);
                    if(prev.startsWith("0")){
                        prev = prev.substring(prev.indexOf("0")+1);
                    }
                    System.out.println("RESULT: " + compressor.decodeVariableByte(prev));
                    prev = "";
                    nextValue = compressor.decodeVariableByte(strResult) + 1;
                }
                else{
                    //System.out.println("string: " + strResult);
                    System.out.println("RESULT: " + compressor.decodeVariableByte(strResult));
                    nextValue = compressor.decodeVariableByte(strResult) + 1;
                }
            }
        }
        // close both channel and file
        channel.close();
        fileinput.close();
    }

    public void testStringCompressionVB() throws IOException {
        Compressor compressor = new Compressor();
        String bitString = compressor.variableByte(60000);
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        if(ba[0]==0){
            ba = Arrays.copyOfRange(ba, 1, ba.length);
            //System.out.println("cut");
        }
        File file = new File("docs/prova.bin");

        //WRITE BINARY OPERATION
        try (RandomAccessFile stream = new RandomAccessFile(file, "rw");
             FileChannel channel = stream.getChannel())
        {
            byte[] ba0 = new BigInteger(compressor.variableByte(0), 2).toByteArray();
            if(ba[0]==0 && ba.length>1){
                ba = Arrays.copyOfRange(ba, 1, ba.length);
                //System.out.println("cut");
            }
            //System.out.println(ba0.length);
            ByteBuffer bufferValue0 = ByteBuffer.allocate(ba0.length);
            bufferValue0.put(ba0);
            bufferValue0.flip();
            channel.write(bufferValue0);
            System.out.println("WRITE: 0");
            byte[] ba4 = new BigInteger(compressor.variableByte(67822), 2).toByteArray();
            //TODO: W A R N I N G
            // toByteArray adds one byte due to the fact that it adds a sign bit 0
            //System.out.println(ba4.length);
            if(ba4[0]==0){
                ba4 = Arrays.copyOfRange(ba4, 1, ba4.length);
                //System.out.println("cut");
            }
            //System.out.println(ba4.length);
            ByteBuffer bufferValue4 = ByteBuffer.allocate(ba4.length);
            bufferValue4.put(ba4);
            bufferValue4.flip();
            channel.write(bufferValue4);
            System.out.println("WRITE: 67822");
            byte[] ba2 = new BigInteger(compressor.variableByte(9), 2).toByteArray();
            //System.out.println(ba2.length);
            ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length);
            bufferValue2.put(ba2);
            bufferValue2.flip();
            channel.write(bufferValue2);
            System.out.println("WRITE: 9");
            ByteBuffer bufferValue = ByteBuffer.allocate(ba.length);
            //System.out.println(ba.length);
            bufferValue.put(ba);
            bufferValue.flip();
            channel.write(bufferValue);
            System.out.println("WRITE: 60000");
            int i = 0;
            while (i<5){
                byte[] ba1 = new BigInteger(compressor.variableByte(5), 2).toByteArray();
                //System.out.println(ba1.length);
                ByteBuffer bufferValue1 = ByteBuffer.allocate(ba1.length);
                bufferValue1.put(ba1);
                bufferValue1.flip();
                channel.write(bufferValue1);
                System.out.println("WRITE: 5");
                i++;
            }
            byte[] ba6 = new BigInteger(compressor.variableByte(10), 2).toByteArray();
            if(ba6[0]==0){
                ba6 = Arrays.copyOfRange(ba6, 1, ba6.length);
                //System.out.println("cut");
            }
            //System.out.println(ba6.length);
            ByteBuffer bufferValue6 = ByteBuffer.allocate(ba6.length);
            bufferValue6.put(ba6);
            bufferValue6.flip();
            channel.write(bufferValue6);
            System.out.println("WRITE: 10");
            byte[] ba5 = new BigInteger(compressor.variableByte(1357345), 2).toByteArray();
            if(ba5[0]==0){
                ba5 = Arrays.copyOfRange(ba5, 1, ba5.length);
                //System.out.println("cut");
            }
            ByteBuffer bufferValue5 = ByteBuffer.allocate(ba5.length);
            bufferValue5.put(ba5);
            bufferValue5.flip();
            channel.write(bufferValue5);
            System.out.println("WRITE: 1357345");
            byte[] ba7 = new BigInteger(compressor.variableByte(8357345), 2).toByteArray();
            if(ba7[0]==0){
                ba7 = Arrays.copyOfRange(ba7, 1, ba7.length);
            }
            ByteBuffer bufferValue7 = ByteBuffer.allocate(ba7.length);
            bufferValue7.put(ba7);
            bufferValue7.flip();
            channel.write(bufferValue7);
            System.out.println("WRITE: 8357345");
            ByteBuffer bufferEnd = ByteBuffer.allocate("\n".getBytes().length);
            bufferEnd.put("\n".getBytes());
            bufferEnd.flip();
            channel.write(bufferEnd);
            System.out.println("Successfully written data to the file");
            stream.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        //READ OPERATION
        // open filechannel
        String path = "docs/prova.bin";
        RandomAccessFile fileinput = new RandomAccessFile(path, "r");;
        FileChannel channel = fileinput.getChannel();
        //set the buffer size
        int bufferSize = 1;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        String prev = "";
        int nextValue = 0;
        // read the data from filechannel
        while (channel.read(buffer) != -1) {
            byte[] input = (buffer.array());
            BigInteger one = new BigInteger(input);
            if (one.compareTo(BigInteger.ZERO) < 0)
                one = one.add(BigInteger.ONE.shiftLeft(8));
            buffer.clear();
            String strResult = one.toString(2);
            System.out.println(strResult);
            if(strResult.equals("1010") && nextValue>10 && prev.equals("")){
                System.out.println("end line!");
                break;
            }
            if(strResult.length() == 8){
                prev+= strResult;
            }
            else if(strResult.equals("0") && nextValue > 0){
                if(!prev.equals("")){
                    System.out.println("RESULT: " + compressor.decodeVariableByte(prev));
                    nextValue = compressor.decodeVariableByte(prev) + 1;
                    prev ="";
                }
            }
            else{
                if(prev!=""){
                    prev+=strResult;
                    if(prev.startsWith("0")){
                        prev = prev.substring(prev.indexOf("0")+1);
                    }
                    System.out.println("RESULT: " + compressor.decodeVariableByte(prev));
                    prev = "";
                    nextValue = compressor.decodeVariableByte(strResult) + 1;
                }
                else{
                    System.out.println("RESULT: " + compressor.decodeVariableByte(strResult));
                    nextValue = compressor.decodeVariableByte(strResult) + 1;
                }
            }
        }
        // close both channel and file
        channel.close();
        fileinput.close();
    }
    */
}