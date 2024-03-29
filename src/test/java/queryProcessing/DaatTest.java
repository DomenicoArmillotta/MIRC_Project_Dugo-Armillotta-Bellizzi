package queryProcessing;

import compression.Compressor;
import fileManager.ConfigurationParameters;
import invertedIndex.LexiconStats;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import preprocessing.PreprocessDoc;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;

public class DaatTest extends TestCase {
    public LexiconStats getPointer(FileChannel channel, String key) throws IOException {
        LexiconStats l = new LexiconStats();
        int entrySize = ConfigurationParameters.LEXICON_ENTRY_SIZE;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
        int lowerBound = 0;
        int upperBound = (int) channel.size()-entrySize;
        /*while(lowerBound!=channel.size()){
            buffer.position(lowerBound);
            ByteBuffer ba = ByteBuffer.allocate(22);
            buffer.get(ba.array(), 0, 22);
            if(ba.hasArray()) {
                byte[] term = new byte[22];
                term = ba.array();
                String value = Text.decode(term).toString();
                value = value.replaceAll("\0", "");
                System.out.println("Found key " + value);
                ByteBuffer bf1 = ByteBuffer.allocate(entrySize-22);
                buffer.get(bf1.array(), 0, entrySize-22);
                l = new LexiconStats(bf1);
                System.out.println(l.getCf() + " " + l.getdF() + " " + l.getOffsetDocid() + " " + l.getDocidsLen()
                        + " " + l.getTermUpperBound() + " " + l.getOffsetSkip() + " " + l.getSkipLen());
            }
            lowerBound+=entrySize;
        }*/
        while (lowerBound <= upperBound) {
            int midpoint = (lowerBound + upperBound) / 2;
            if(midpoint%entrySize!=0){
                midpoint += midpoint%entrySize;
            }
            buffer.position(midpoint);
            ByteBuffer ba = ByteBuffer.allocate(22);
            buffer.get(ba.array(), 0, 22);
            String value = Text.decode(ba.array());
            value = value.replaceAll("\0", "");
            //System.out.println(value + " " + lowerBound + " " + upperBound);
            if (value.equals(key)) {
                System.out.println("Found key " + key + " at position " + midpoint);
                ByteBuffer bf1 = ByteBuffer.allocate(entrySize-22);
                buffer.get(bf1.array(), 0, entrySize-22);
                l = new LexiconStats(bf1);
                System.out.println(l.getCf() + " " + l.getdF() + " " + l.getOffsetDocid() + " " + l.getDocidsLen()
                        + " " + l.getTermUpperBound() + " " + l.getOffsetSkip() + " " + l.getSkipLen());
                break;
            } else if (key.compareTo(value) < 0) {
                upperBound = midpoint - entrySize;
            } else if(value.equals("") || (key.compareTo(value) > 0)) {
                lowerBound = midpoint + entrySize;
            }
        }
        return l;
    }

    public void testFullRead() throws IOException {
        String lexiconPath = "docs/lexicon.txt";
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        FileChannel channel = lexFile.getChannel();
        LexiconStats l = new LexiconStats();
        int entrySize = ConfigurationParameters.LEXICON_ENTRY_SIZE;
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size());
        int lowerBound = 0;
        int upperBound = (int) channel.size()-entrySize;
        while(lowerBound!=channel.size()){
            buffer.position(lowerBound);
            ByteBuffer ba = ByteBuffer.allocate(22);
            buffer.get(ba.array(), 0, 22);
            if(ba.hasArray()) {
                byte[] term = new byte[22];
                term = ba.array();
                String value = Text.decode(term).toString();
                value = value.replaceAll("\0", "");
                System.out.println("-"+value);
                ByteBuffer bf1 = ByteBuffer.allocate(entrySize-22);
                buffer.get(bf1.array(), 0, entrySize-22);
                l = new LexiconStats(bf1);
                /*System.out.println(l.getCf() + " " + l.getdF() + " " + l.getOffsetDocid() + " " + l.getDocidsLen()
                        + " " + l.getTermUpperBound() + " " + l.getOffsetSkip() + " " + l.getSkipLen());*/
            }
            lowerBound+=entrySize;
        }
    }

    public void testLexTermRead() throws IOException {
        //String lexiconPath = "docs/lexiconTot.txt";
        String lexiconPath = "docs/lexicon.txt";
        HashMap<String, LexiconStats> lexicon = new HashMap<>();
        PreprocessDoc preprocessing = new PreprocessDoc();
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        FileChannel lexChannel = lexFile.getChannel();
        String query = "president zzz";
        List<String> proQuery = preprocessing.preprocessDocument(query);
        for(String term: proQuery){
            System.out.println(term);
            LexiconStats l1 = getPointer(lexChannel, term);
        }
    }

    public void testLexiconRead() throws IOException {
        //String lexiconPath = "docs/lexiconTot.txt";
        String lexiconPath = "docs/lexicon.txt";
        HashMap<String, LexiconStats> lexicon = new HashMap<>();
        PreprocessDoc preprocessing = new PreprocessDoc();
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        FileChannel lexChannel = lexFile.getChannel();
        RandomAccessFile inDocsFile = new RandomAccessFile(new File("docs/docids.txt"),"rw");
        FileChannel docChannel = inDocsFile.getChannel();
        RandomAccessFile inTfFile = new RandomAccessFile(new File("docs/tfs.txt"),"rw");
        FileChannel tfChannel = inTfFile.getChannel();
        String query = "bile";
        List<String> proQuery = preprocessing.preprocessDocument(query);
        LexiconStats l1 = getPointer(lexChannel, proQuery.get(0));
        RandomAccessFile skipFile = new RandomAccessFile(new File("docs/skipInfo.txt"), "rw");
        FileChannel skipChannel = skipFile.getChannel();
        skipChannel.position(l1.getOffsetSkip());
        ByteBuffer readBuffer = ByteBuffer.allocate(l1.getSkipLen());
        skipChannel.read(readBuffer);
        int tot = 0;
        while(tot < l1.getSkipLen()) {
            readBuffer.position(tot);
            int endocid1 = readBuffer.getInt();
            int skiplen1 = readBuffer.getInt();
            int skiplen2 = readBuffer.getInt();
            System.out.println(endocid1 + " " + skiplen1 + " " + skiplen2);
            tot+=12;
        }
        Compressor c = new Compressor();
        long offsetDoc = l1.getOffsetDocid();
        long offsetTf = l1.getOffsetTf();
        int docLen = l1.getDocidsLen();
        int tfLen = l1.getTfLen();
        docChannel.position(offsetDoc);
        ByteBuffer docids = ByteBuffer.allocate(docLen);
        docChannel.read(docids);
        List<Integer> decompressedDocids = c.variableByteDecode(docids.array());
        tfChannel.position(offsetTf);
        ByteBuffer tfs = ByteBuffer.allocate(tfLen);
        tfChannel.read(tfs);
        List<Integer> decompressedTfs = c.unaryDecode(tfs.array());
        System.out.println("Docids: "  + decompressedDocids);
        System.out.println(decompressedDocids.size());
        System.out.println("Tfs: " + decompressedTfs);
        System.out.println(decompressedTfs.size());
        /*long offsetDoc = l1.getOffsetDocid();
        long offsetTf = l1.getOffsetTf();
        int docLen = l1.getDocidsLen();
        int tfLen = l1.getTfLen();
        docChannel.position(offsetDoc);
        ByteBuffer docids = ByteBuffer.allocate(docLen);
        docChannel.read(docids);
        tfChannel.position(offsetTf);
        ByteBuffer tfs = ByteBuffer.allocate(tfLen);
        tfChannel.read(tfs);
        int offDoc = 0;
        int offTf = 0;
        int listSize =docLen/4;
        for(int i = 0; i < listSize; i++){
            docids.position(offDoc);
            tfs.position(offTf);
            int docId = docids.getInt();
            int tf = tfs.getInt();
            System.out.println(docId + " " +  tf);
            offDoc+=4;
            offTf+=4;
        }*/
        /*
        for(String term: proQuery){
            LexiconStats l = getPointer(lexChannel, term);
            lexicon.put(term, l);
        }*/
        /*
        String query2 = "bile acid stomach american people table dratini";
        for(String term: query2.split(" ")){
            LexiconStats l = getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        Compressor c = new Compressor();
        long offsetDoc = l1.getOffsetDocid();
        long offsetTf = l1.getOffsetTf();
        int docLen = l1.getDocidsLen();
        int tfLen = l1.getTfLen();
        docChannel.position(offsetDoc);
        ByteBuffer docids = ByteBuffer.allocate(docLen);
        docChannel.read(docids);
        List<Integer> decompressedDocids = c.variableByteDecode(docids.array());
        tfChannel.position(offsetTf);
        ByteBuffer tfs = ByteBuffer.allocate(tfLen);
        tfChannel.read(tfs);
        List<Integer> decompressedTfs = c.unaryDecode(tfs.array());
        System.out.println("Docids: "  + decompressedDocids);
        System.out.println(decompressedDocids.size());
        System.out.println("Tfs: " + decompressedTfs);
        System.out.println(decompressedTfs.size());
        int prec = 0;
        for(int i : decompressedDocids){
            if(i<=prec && i!=0){
                System.out.println("ERROR");
                return;
            }
            prec = i;
        }*/
        /*for(String term: proQuery){
            LexiconStats l = getPointer(lexChannel, term);
            lexicon.put(term, l);
        }*/
        /*RandomAccessFile skipFile = new RandomAccessFile(new File("docs/skipInfo.txt"), "rw");
        FileChannel skipChannel = skipFile.getChannel();
        for(LexiconStats l: lexicon.values()) {
            skipChannel.position(l.getOffsetSkip());
            ByteBuffer readBuffer = ByteBuffer.allocate(l.getSkipLen());
            skipChannel.read(readBuffer);
            readBuffer.position(0);
            int endocid1 = readBuffer.getInt();
            int skiplen1 = readBuffer.getInt();
            System.out.println(endocid1 + " " + skiplen1);
        }*/
        /*LexiconStats l = getPointer(lexChannel, "bile");
        LexiconStats l1 = getPointer(lexChannel, "american");
        LexiconStats l2 = getPointer(lexChannel, "medi");
        LexiconStats l3 = getPointer(lexChannel, "hello");
        LexiconStats l4 = getPointer(lexChannel, "peopl");*/
        /*LexiconStats l5 = getPointer(lexChannel, "build");
        LexiconStats l6 = getPointer(lexChannel, "face");
        LexiconStats l7 = getPointer(lexChannel, "abdomin");
        LexiconStats l8 = getPointer(lexChannel, "legal");
        LexiconStats l9 = getPointer(lexChannel, "dog");
        LexiconStats l10 = getPointer(lexChannel, "medic");*/
    }

    public void testConjunctiveDaat() throws IOException {
        Daat d = new Daat();
        String query = "bile acid";
        //System.out.println(d.conjunctiveDaat(query,10));
        query = "american people";
        //System.out.println(d.conjunctiveDaat(query,10));
        query = "france kidney";
        System.out.println(d.conjunctiveDaat(query,10, true));
    }

    public void testDisjunctiveDaat() throws IOException {
        Daat d = new Daat();
        String query = "bile acid stomach";
        //query = "american people";
        //query = "bile acid stomach american people table dratini";
        //System.out.println(d.disjunctiveDaat(query,10));
        //Some tests:
        //[30426=8.283077691011705, 33136=8.151426009815147, 28819=8.11531806595153, 28817=8.01551599334571, 15218=7.734391511217466, 33137=7.499009903437889, 11601=7.451369281607839, 11598=7.197704376026827, 67384=7.107704678409375, 64=6.949409115587326]
        //[30426=8.283077691011705, 33136=8.151426009815147, 28819=8.11531806595153, 28817=8.01551599334571, 15218=7.734391511217466, 33137=7.499009903437889, 11601=7.451369281607839, 11598=7.197704376026827, 67384=7.107704678409375, 64=6.949409115587326]
        //[30426=8.283077691011705, 33136=8.151426009815147, 28819=8.11531806595153, 28817=8.01551599334571, 15218=7.734391511217466, 33137=7.499009903437889, 11601=7.451369281607839, 11598=7.197704376026827, 67384=7.107704678409375, 64=6.949409115587326]
        //query = "american people";
        long start = System.currentTimeMillis();
        System.out.println(d.disjunctiveDaat(query,10, true));
        long end = System.currentTimeMillis();
        long time = end - start;
        System.out.println("TIME: " + time + "ms");
        //query = "france kidney";
        //System.out.println(d.disjunctiveDaat(query,10));
    }

    public void testSkipInfo() throws IOException {
        String lexiconPath = "docs/lexiconTot.txt";
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        FileChannel lexChannel = lexFile.getChannel();
        LexiconStats l1 = getPointer(lexChannel, "bile");
        LexiconStats l = getPointer(lexChannel, "american");
        RandomAccessFile skipFile = new RandomAccessFile(new File("docs/skipInfo.txt"), "rw");
        FileChannel skipChannel = skipFile.getChannel();
        skipChannel.position(l.getOffsetSkip());
        ByteBuffer readBuffer = ByteBuffer.allocate(l.getSkipLen());
        skipChannel.read(readBuffer);
        readBuffer.position(0);
        int endocid1 = readBuffer.getInt();
        int skiplen1 = readBuffer.getInt();
        System.out.println(endocid1 + " " + skiplen1);
    }
}