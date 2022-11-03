package indexing;

import inverted_index.Compressor;
import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class SPIMI_InvertTest extends TestCase {
    public void testSpimi() throws IOException {
        /*
        SPIMI spimi = new SPIMI();
        String path = "docs/collection_test.tsv";
        spimi.spimiInvertBlockCompression(path,10);
         */



        //test
        Compressor compressor = new Compressor();
        String bitString = compressor.unary(4);
        System.out.println(bitString);
        System.out.println(compressor.decodeUnary(bitString));
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        System.out.println("scrivo = "+ba);
        File file = new File("docs/prova.txt");

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
        String path = "docs/prova.txt";
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
            System.out.println(strResult);
            System.out.println(compressor.decodeUnary(strResult));
        }

        // clode both channel and file
        channel.close();
        fileinput.close();


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
}