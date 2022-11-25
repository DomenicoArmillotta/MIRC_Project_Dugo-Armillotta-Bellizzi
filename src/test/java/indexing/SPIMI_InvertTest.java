package indexing;

import inverted_index.Compressor;
import inverted_index.Posting;
import junit.framework.TestCase;
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
import java.util.*;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class SPIMI_InvertTest extends TestCase {
    public void testSpimi() throws IOException {
        SPIMI spimi = new SPIMI();
        String path = "docs/collection_test.tsv";
        //spimi.spimiInvertBlockCompression(path,4);
        spimi.spimiInvertBlockWithRamUsage(path);
    }

    public void testSpimiBin() throws IOException {
        SPIMI spimi = new SPIMI();
        String path = "docs/collection_test.tsv";
        //spimi.spimiInvertBlockWithRamUsageCompressed(path);
    }
    public void testSpimiCompression() throws IOException {
        Compressor compressor = new Compressor();
        //--> unario
        String bitString = compressor.unary(4);
        System.out.println(bitString);
        //--> byte
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        System.out.println(ba);
        BigInteger one = new BigInteger(ba);
        //--> unario
        String strResult = one.toString(2);
        System.out.println(strResult);
        //--> intero
        System.out.println(compressor.decodeUnary(strResult));

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

    public void testSinglePassInMemory() throws IOException {
        //use DBMaker to create a DB object stored on disk
        //provide output location of list
        String path = "docs/collection_test.tsv";
        DB db = DBMaker.fileDB("docs/testPointers.db").make();
        HTreeMap<String, List<Posting>> lexicon = db
                .hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        HTreeMap<String, Integer> documentIndex = db
                .hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
        int docid = 0;
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
                    lexicon.put(term, new ArrayList<Posting>());
                    //addPosting(invertedIndex, term, doc_id, 1);
                    //Posting p = new Posting(docid, 1);
                    if(lexicon.get(term).isEmpty()){
                        List<Posting> list = lexicon.get(term);
                        list = (ArrayList<Posting>) db.indexTreeList("postings_"+term, Serializer.JAVA).createOrOpen();
                        HTreeMap<Integer, Integer> postingList = db.hashMap("postings_"+term)
                                .keySerializer(Serializer.INTEGER)
                                .valueSerializer(Serializer.INTEGER)
                                .createOrOpen();
                        List<Posting> l = new ArrayList<>();
                        lexicon.put(term, list);
                        l.add(new Posting(docid, 1));
                        list.add(new Posting(docid, 1));
                        postingList.put(docid, 1);
                        System.out.println(term + " " + list);
                        //invertedIndex.put(term,l);
                    }
                    else {
                        List<Posting> l = lexicon.get(term);
                        boolean change = false;
                        for (Posting posting : l) {
                            if (posting.getDocumentId() == docid) {
                                posting.addOccurrence();
                                change = true;
                                //return;
                            }
                        }
                        if(!change) {
                            l.add(new Posting(docid, 1));
                        }
                    }
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
        DB readDb = DBMaker.fileDB("docs/testPointers.db").make();
        HTreeMap<String, List<Posting>> readLexicon = readDb.hashMap("lexicon")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        HTreeMap<String, Integer> readDocIndex = readDb.hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
        List<Posting> list = readLexicon.get("bile");
        System.out.println(list);
        //close to protect from data corruption
        db.close();
    }

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
        int docid = 0;
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
        /*for (int i = 0; i < 1000; i++) {
            System.out.println(readList.get(i));
        }*/
        /*for(int i = 0; i < readDocIndex.size(); i++){
            System.out.println(readDocIndex.get(i));
        }*/

        //close to protect from data corruption
        db.close();

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

    public void addPosting(HTreeMap<String, List<Posting>> index, String term, int docid, int freq){
        if(index.get(term) == null){
            List<Posting> l = new ArrayList<>();
            l.add(new Posting(docid, freq));
            index.put(term,l);
        }
        else{
            List<Posting> l = index.get(term);
            for(Posting posting: l){
                if(posting.getDocumentId() == docid){
                    posting.addOccurrence();
                    return;
                }
            }
            index.get(term).add(new Posting(docid,freq));
        }
    }

    public void testStringCompression() throws IOException {
        Compressor compressor = new Compressor();
        String bitString = compressor.unary(11);
        System.out.println(bitString);
        System.out.println(compressor.decodeUnary(bitString));
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        System.out.println("scrivo = "+ba);
        File file = new File("docs/prova.bin");

        //write binary
        try (RandomAccessFile stream = new RandomAccessFile(file, "rw");
             FileChannel channel = stream.getChannel())
        {
            ByteBuffer buffer = ByteBuffer.allocate(ba.length);
            buffer.put(ba);
            buffer.flip();
            channel.write(buffer);

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
        int bufferSize = 1024;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

        // read the data from filechannel
        while (channel.read(buffer) != -1) {
            byte[]  input = (buffer.array());
            BigInteger one = new BigInteger(input);
            //--> unario
            String strResult = one.toString(2);
            int index = strResult.lastIndexOf("1")+2;
            strResult = strResult.substring(0,index);
            System.out.println(strResult);
            System.out.println(compressor.decodeUnary(strResult));
        }

        // clode both channel and file
        channel.close();
        fileinput.close();

    }

    public void testStringCompressionWithSpace() throws IOException {
        Compressor compressor = new Compressor();
        String bitString = compressor.unary(4);
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        System.out.println("scrivo = "+ba);
        File file = new File("docs/prova.bin");

        //write binary
        try (RandomAccessFile stream = new RandomAccessFile(file, "rw");
             FileChannel channel = stream.getChannel())
        {
            ByteBuffer bufferValue = ByteBuffer.allocate(ba.length);
            bufferValue.put(ba);
            bufferValue.flip();
            channel.write(bufferValue);
            int i = 0;
            while (i<5){
                byte[] ba2 = new BigInteger(compressor.unary(5), 2).toByteArray();
                ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length);
                //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
                //bufferValue2.put(System.getProperty("line.separator").getBytes());
                bufferValue2.put(ba2);
                bufferValue2.flip();
                channel.write(bufferValue2);
                i++;
            }
            byte[] ba3 = new BigInteger(compressor.unary(11), 2).toByteArray();
            System.out.println("scrivo unary(11): "+ ba3);
            ByteBuffer bufferValue2 = ByteBuffer.allocate(ba3.length);
            //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
            //bufferValue2.put(System.getProperty("line.separator").getBytes());
            bufferValue2.put(ba3);
            bufferValue2.flip();
            channel.write(bufferValue2);
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

        //read
        // open filechannel
        String path = "docs/prova.bin";
        RandomAccessFile fileinput = new RandomAccessFile(path, "r");;
        FileChannel channel = fileinput.getChannel();

        //set the buffer size
        int bufferSize = 1;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        String prev = "";
        // read the data from filechannel
        while (channel.read(buffer) != -1) {
            byte[]  input = (buffer.array());
            BigInteger one = new BigInteger(input);
            if (one.compareTo(BigInteger.ZERO) < 0)
                one = one.add(BigInteger.ONE.shiftLeft(8));
            //--> unario
            String strResult = one.toString(2);
            if(strResult.equals("1010")){
                System.out.println("end line!");
                break;
            }
            if(strResult.indexOf("0")==-1 || strResult.equals("0")){
                System.out.println("not zero: "+ strResult);
                prev+=strResult;
            }
            else{
                if(prev!=""){
                    System.out.println("previous: " + prev);
                    prev+=strResult;
                    System.out.println("ending string: " + strResult);
                    System.out.println(prev);
                    if(prev.startsWith("0")){
                        prev = prev.substring(prev.indexOf("0")+1);
                    }
                    System.out.println(compressor.decodeUnary(prev));
                    prev = "";
                }
                else{
                    System.out.println("string: " + strResult);
                    System.out.println(compressor.decodeUnary(strResult));
                }
            }
            buffer.clear();
        }
        // clode both channel and file
        channel.close();
        fileinput.close();
    }

    public void testStringCompressionVBOld() throws IOException {
        Compressor compressor = new Compressor();
        String bitString = compressor.variableByteNew(60000);
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        File file = new File("docs/prova.bin");

        //write binary
        try (RandomAccessFile stream = new RandomAccessFile(file, "rw");
             FileChannel channel = stream.getChannel())
        {
            byte[] ba0 = new BigInteger(compressor.variableByteNew(0), 2).toByteArray();
            System.out.println(ba0.length);
            ByteBuffer bufferValue0 = ByteBuffer.allocate(ba0.length);
            //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
            //bufferValue2.put(System.getProperty("line.separator").getBytes());
            bufferValue0.put(ba0);
            bufferValue0.flip();
            channel.write(bufferValue0);
            System.out.println("WRITE: 0");
            byte[] ba4 = new BigInteger(compressor.variableByteNew(67822), 2).toByteArray();
            System.out.println(ba4.length);
            ByteBuffer bufferValue4 = ByteBuffer.allocate(ba4.length);
            bufferValue4.put(ba4);
            bufferValue4.flip();
            channel.write(bufferValue4);
            System.out.println("WRITE: 67822");
            byte[] ba2 = new BigInteger(compressor.variableByteNew(9), 2).toByteArray();
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
                byte[] ba1 = new BigInteger(compressor.variableByteNew(5), 2).toByteArray();
                System.out.println(ba1.length);
                ByteBuffer bufferValue1 = ByteBuffer.allocate(ba1.length);
                //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
                //bufferValue2.put(System.getProperty("line.separator").getBytes());
                bufferValue1.put(ba1);
                bufferValue1.flip();
                channel.write(bufferValue1);
                System.out.println("WRITE: 5");
                i++;
            }/*
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
            byte[] ba6 = new BigInteger(compressor.variableByteNew(10), 2).toByteArray();
            System.out.println(ba6.length);
            ByteBuffer bufferValue6 = ByteBuffer.allocate(ba6.length);
            //ByteBuffer bufferValue2 = ByteBuffer.allocate(ba2.length + System.lineSeparator().length());
            //bufferValue2.put(System.getProperty("line.separator").getBytes());
            bufferValue6.put(ba6);
            bufferValue6.flip();
            channel.write(bufferValue6);
            System.out.println("WRITE: 10");
            byte[] ba5 = new BigInteger(compressor.variableByteNew(1357), 2).toByteArray();
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
            }
            /*if(strResult.equals("1101") && prev.equals("1010")) {
                System.out.println("new line! " + strResult);
                break;
            }*/
            if(strResult.length() == 8 || (strResult.equals("0") && nextValue!=0)){
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
                    System.out.println("RESULT: " + compressor.decodeVariableByteNew(prev));
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
        String bitString = compressor.variableByteNew(60000);
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
            byte[] ba0 = new BigInteger(compressor.variableByteNew(0), 2).toByteArray();
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
            byte[] ba4 = new BigInteger(compressor.variableByteNew(67822), 2).toByteArray();
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
            byte[] ba2 = new BigInteger(compressor.variableByteNew(9), 2).toByteArray();
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
                byte[] ba1 = new BigInteger(compressor.variableByteNew(5), 2).toByteArray();
                //System.out.println(ba1.length);
                ByteBuffer bufferValue1 = ByteBuffer.allocate(ba1.length);
                bufferValue1.put(ba1);
                bufferValue1.flip();
                channel.write(bufferValue1);
                System.out.println("WRITE: 5");
                i++;
            }
            byte[] ba6 = new BigInteger(compressor.variableByteNew(10), 2).toByteArray();
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
            byte[] ba5 = new BigInteger(compressor.variableByteNew(1357345), 2).toByteArray();
            if(ba5[0]==0){
                ba5 = Arrays.copyOfRange(ba5, 1, ba5.length);
                //System.out.println("cut");
            }
            ByteBuffer bufferValue5 = ByteBuffer.allocate(ba5.length);
            bufferValue5.put(ba5);
            bufferValue5.flip();
            channel.write(bufferValue5);
            System.out.println("WRITE: 1357345");
            byte[] ba7 = new BigInteger(compressor.variableByteNew(8357345), 2).toByteArray();
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
                    System.out.println("RESULT: " + compressor.decodeVariableByteNew(prev));
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
                    System.out.println("RESULT: " + compressor.decodeVariableByteNew(prev));
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

    public static String byteFormat(String src) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < src.length(); i++) {

            char chr = src.charAt(i);
            String format = String.format("%8s", Integer.toBinaryString(chr)).replace(' ', '0');
            sb.append(format);
        }
        return sb.toString();
    }

    /*byte[] ba = new BigInteger(bitString, 2).toByteArray();
    ByteBuffer bufferValue = ByteBuffer.allocate(ba.length);
                                bufferValue.put(ba);
                                bufferValue.flip();
                                channel.write(bufferValue);
    ByteBuffer bufferSpace = ByteBuffer.allocate(ba.length);
                                bufferSpace.put(" ".getBytes());
                                bufferSpace.flip();
                                channel.write(bufferSpace);*/

    public static byte[] compress(String text) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            OutputStream out = new DeflaterOutputStream(baos);
            out.write(text.getBytes("UTF-8"));
            out.close();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return baos.toByteArray();
    }

    public static String decompress(byte[] bytes) {
        InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            byte[] buffer = new byte[8192];
            int len;
            while((len = in.read(buffer))>0)
                baos.write(buffer, 0, len);
            return new String(baos.toByteArray(), "UTF-8");
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public void testBinConversion(){
        String text = "11101011100001100001";
        String binary = new BigInteger(text.getBytes()).toString(2);
        System.out.println("As binary: "+binary);

        String text2 = new String(new BigInteger(binary, 2).toByteArray());
        System.out.println("As text: "+text2);
        char[] cArray=text.toCharArray();

        StringBuilder sb=new StringBuilder();

        for(char c:cArray)
        {
            String cBinaryString=Integer.toBinaryString((int)c);
            sb.append(cBinaryString);
        }

        System.out.println(sb.toString());
    }
}