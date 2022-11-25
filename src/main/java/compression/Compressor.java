package compression;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class Compressor {

    public String variableByte(int x) {
        //TODO: implement
       return "";
    }

    public int decodeVariableByteOld(String bitString){
       //TODO: implement:
        return 0;
    }

    public byte[] stringCompressionWithVariableByte(int doc){
        String bitString = variableByte(doc);
        //System.out.println(bitString);
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        if(ba[0]==0 && ba.length>1){ //because toByteArray() adds one bit 0 for the sign, so we have one extra byte
            ba = Arrays.copyOfRange(ba, 1, ba.length);
        }
        return ba;

    }

    //TODO: define the return value(s)
    public void decompressVariableByte(int offset, int end, String path) throws IOException {
        RandomAccessFile fileinput = new RandomAccessFile(path, "r");
        //FileChannel channel = fileinput.getChannel();
        //set the buffer size
        int bufferSize = 1;
        Path filepath = Paths.get(path);
        SeekableByteChannel channel = Files.newByteChannel(filepath, StandardOpenOption.READ);
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        channel = channel.position(offset);
        int nextValue = 0;
        int cont = 0;
        int n = 0;
        // read the data from filechannel
        //TODO: complete
        // close both channel and file
        channel.close();
        fileinput.close();
    }

}
