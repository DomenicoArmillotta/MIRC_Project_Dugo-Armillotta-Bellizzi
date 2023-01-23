package queryProcessing;

import fileManager.ConfigurationParameters;
import invertedIndex.InvertedIndex;
import invertedIndex.LexiconStats;
import junit.framework.TestCase;
import org.apache.hadoop.io.Text;
import preprocessing.PreprocessDoc;
import utility.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
                System.out.println(value + " " + lowerBound);
            }
            lowerBound+=66;
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
            } else {
                lowerBound = midpoint + entrySize;
            }
        }
        return l;
    }

    public void testLexiconRead() throws IOException {
        //String lexiconPath = "docs/lexicon.txt";
        String lexiconPath = "docs/lexiconTot.txt";
        HashMap<String, LexiconStats> lexicon = new HashMap<>();
        PreprocessDoc preprocessing = new PreprocessDoc();
        RandomAccessFile lexFile = new RandomAccessFile(new File(lexiconPath), "rw");
        FileChannel lexChannel = lexFile.getChannel();
        String query = "how often do american people eat";
        List<String> proQuery = preprocessing.preprocess_doc_optimized(query);
        for(String term: proQuery){
            LexiconStats l = getPointer(lexChannel, term);
            lexicon.put(term, l);
        }
        RandomAccessFile skipFile = new RandomAccessFile(new File("docs/skipInfo.txt"), "rw");
        FileChannel skipChannel = skipFile.getChannel();
        for(LexiconStats l: lexicon.values()) {
            skipChannel.position(l.getOffsetSkip());
            ByteBuffer readBuffer = ByteBuffer.allocate(l.getSkipLen());
            skipChannel.read(readBuffer);
            readBuffer.position(0);
            int endocid1 = readBuffer.getInt();
            int skiplen1 = readBuffer.getInt();
            System.out.println(endocid1 + " " + skiplen1);
        }
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
        String query = "bile acid stomach";
        System.out.println(d.conjunctiveDaat(query,10));
        /*query = "american people";
        System.out.println(d.conjunctiveDaat(query,10));*/
        query = "france kidney";
        System.out.println(d.conjunctiveDaat(query,10));
    }

    public void testDisjunctiveDaat() throws IOException {
        Daat d = new Daat();
        String query = "bile acid stomach american";
        d.disjunctiveDaat(query,10);
        /*query = "american people";
        System.out.println(d.conjunctiveDaat(query,10));*/
        /*query = "france kidney";
        d.disjunctiveDaat(query,10);*/
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