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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static utility.Utils.addByteArray;

public class Compressor {

    public BitSet unaryEncode(int x) {
        //TODO: implement
        BitSet b = new BitSet(x);
        b.set(0,x-1);
        return b;
    }

    public int unaryDecodeNumber(BitSet b){
        return b.length();
    }

    public List<Integer> unaryDecode(BitSet b){
        List<Integer> numbers = new ArrayList<>();
        int n = 0;
        for(int i = 0; i < b.length(); i++) {
            n++; //increase by 1 each time we find a bit
            if (!b.get(i)) { //if the bit is not set then we have the end of a number
                numbers.add(n);
                n = 0;
            }
            //if the bit is set we go on
        }
        return numbers;
    }

    //TODO: we need methods to convert byte arrays to Bitset!! Here are some examples found online:
    //FIRST EXAMPLE:
    public BitSet byteToBits(byte[] bytearray){
        BitSet returnValue = new BitSet(bytearray.length*8);
        ByteBuffer  byteBuffer = ByteBuffer.wrap(bytearray);
        //System.out.println(byteBuffer.asIntBuffer().get(1));
        //Hexadecimal values used are Big-Endian, because Java is Big-Endian
        for (int i = 0; i < bytearray.length; i++) {
            byte thebyte = byteBuffer.get(i);
            for (int j = 0; j <8 ; j++) {
                returnValue.set(i*8+j,isBitSet(thebyte,j));
            }
        }
        return returnValue;
    }

    private static Boolean isBitSet(byte b, int bit)
    {
        return (b & (1 << bit)) != 0;
    }

    //SECOND EXAMPLE:

    // Returns a bitset containing the values in bytes.
    public static BitSet fromByteArray(byte[] bytes) {
        BitSet bits = new BitSet();
        for (int i = 0; i < bytes.length * 8; i++) {
            if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
                bits.set(i);
            }
        }
        return bits;
    }

    //TODO: decidere se per compressione e decompressione unaria ci serve una lista di numeri da (de)codificare
    // o se lavoriamo solo su singoli numeri

    //TODO: implement the following functions
    /*
        def VariableByteEncodeNumber(n):
            byte_list = []
            while True:
                #prepend:
                byte_list = [n%128] + byte_list
                #byte_list.insert(0,n%128)
                if n < 128:
                    break
                n = int(n/128)
            byte_list[len(byte_list)-1] += 128
            return byte_list
     */

    public byte[] variableByteEncodeNumber(int n){
        byte[] b = new byte[1024];
        while(true){
            b = addByteArray(ByteBuffer.allocate(1).putInt(n%128).array(), b);
            if(n<128){
                break;
            }
            n = n/128;
        }
        b[b.length-1] += 128;
        return b;
    }

    /*
        def VariableByteEncode(n_list):
            bytestream = []
            for n in n_list:
                byte_list = VariableByteEncodeNumber(n)
                bytestream.extend(byte_list)
            return bytestream
     */
    public byte[] variableByteEncode(int[] n_list){
        byte[] byteStream = new byte[1024];
        for(int n: n_list){
            byte[] byteList = variableByteEncodeNumber(n);
            addByteArray(byteStream, byteList);
        }
        return byteStream;
    }
    /*
        def VariableByteDecode(bs):
            numbers = []
            n = 0
            for i in range(len(bs)):
                if bs[i] < 128:
                    n = 128*n + bs[i]
                else:
                    n = 128*n + bs[i] - 128
                    numbers.append(n)
                    n = 0
            return numbers
     */

    public List<Integer> variableByteDecode(byte[] bs){
        List<Integer> numbers = new ArrayList<>();
        int n = 0;
        for(int i = 0; i < bs.length; i++){
            if(bs[i] < 128){
                n = 128*n + bs[i];
            }
            else{
                n = 128*n + bs[i] - 128;
                numbers.add(n);
                n = 0;
            }
        }
        return numbers;
    }

    public String variableByte(int x) {
        //TODO: implement
       return "";
    }

    public byte[] variableByteB(int x) {
        //TODO: implement
        byte[] b = new byte[1024];
        return b;
    }

    public int decodeVariableByte(String bitString){
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
