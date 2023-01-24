package compression;

import org.apache.commons.lang3.ArrayUtils;

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

    public byte[] unaryEncode(int x) {
        if(x==1){
            byte[] res = new byte[1];
            return res;
        }
        BitSet b = new BitSet(x);
        b.set(1,x);
        return b.toByteArray();
    }

    public int unaryDecodeNumber(BitSet b){
        return b.length();
    }

    public List<Integer> unaryDecode(byte[] b){
        List<Integer> numbers = new ArrayList<>();
        //ArrayUtils.reverse(b); //forse non serve
        int n = 0;
        for(int i = 0; i < b.length; i++) {
            if(b[i] == 0x00){
                numbers.add(1);
                n=0;
            }
            else if(b[i]!= 0xFF && ((b[i] & 0x01) == 0)){
                BitSet bs = BitSet.valueOf(new byte[]{b[i]});
                numbers.add(bs.length()+n);
                n=0;
            }
            else if(b[i] == 0xFF){
                n+= 8;
            }
            else{
                BitSet bs = BitSet.valueOf(new byte[]{b[i]});
                n+=bs.length();
            }
        }
        return numbers;
    }

    public List<Integer> unaryDecodeBlock(byte[] b, int num){
        List<Integer> numbers = new ArrayList<>();
        //ArrayUtils.reverse(b); //forse non serve
        int n = 0;
        int cont = 0;
        for(int i = 0; i < b.length; i++) {
            if(cont == num) break;
            if(b[i] == 0x00){
                numbers.add(1);
                cont++;
                n=0;
            }
            else if(b[i]!= 0xFF && ((b[i] & 0x01) == 0)){
                BitSet bs = BitSet.valueOf(new byte[]{b[i]});
                numbers.add(bs.length()+n);
                cont++;
                n=0;
            }
            else if(b[i] == 0xFF){
                n+= 8;
            }
            else{
                BitSet bs = BitSet.valueOf(new byte[]{b[i]});
                n+=bs.length();
            }
        }
        return numbers;
    }

    /*public int unaryDecodeAtPosition(byte[] b, int num){
        List<Integer> numbers = new ArrayList<>();
        //ArrayUtils.reverse(b); //forse non serve
        int n = 0;
        int cont = 0;
        for(int i = 0; i < b.length; i++) {
            if(b[i] == 0x00){
                cont++;
                if(cont == num){
                    return 1;
                }
                n=0;
            }
            else if(b[i]!= 0xFF && ((b[i] & 0x01) == 0)){
                BitSet bs = BitSet.valueOf(new byte[]{b[i]});
                cont++;
                int number = bs.length()+n;
                //System.out.println("Number: " +number);
                if(cont == num){
                    return bs.length()+n;
                }
                n=0;
            }
            else if(b[i] == 0xFF){
                n+= 8;
            }
            else{
                BitSet bs = BitSet.valueOf(new byte[]{b[i]});
                n+=bs.length();
            }
        }
        return 0;
    }*/

    //TODO: we need methods to convert byte arrays to Bitset!! Here are some examples found online: --> non servono più
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

    public byte[] variableByteEncodeNumber(int n){
        byte[] b = new byte[0];
        while(true){
            b = addByteArray(b, ByteBuffer.allocate(1).put((byte) (n % 128)).array());
            if(n<128){
                break;
            }
            n = n/128;
        }
        b[0] += 128;
        ArrayUtils.reverse(b);
        return b;
    }

    public byte[] variableByteEncode(int[] n_list){
        byte[] byteStream = new byte[0];
        for(int n: n_list){
            byteStream = addByteArray(byteStream, variableByteEncodeNumber(n));
        }
        return byteStream;
    }

    public List<Integer> variableByteDecode(byte[] bs){
        List<Integer> numbers = new ArrayList<>();
        int n = 0;
        for(int i = 0; i < bs.length; i++){
            if((bs[i] & 0x80) < 128){ //we check if the leftmost bit is not set
                n = 128*n + bs[i];
            }
            else{
                n = 128*n + (256+bs[i]) - 128; //shift because the value is negative
                numbers.add(n);
                n = 0;
            }

        }
        return numbers;
    }

    public List<Integer> variableByteDecodeBlock(byte[] bs, int num){
        List<Integer> numbers = new ArrayList<>();
        int n = 0;
        int cont = 0;
        for(int i = 0; i < bs.length; i++){
            if(cont == num) break;
            if((bs[i] & 0x80) < 128){ //we check if the leftmost bit is not set
                n = 128*n + bs[i];
            }
            else{
                n = 128*n + (256+bs[i]) - 128; //shift because the value is negative
                numbers.add(n);
                n = 0;
                cont++;
            }

        }
        return numbers;
    }
}
