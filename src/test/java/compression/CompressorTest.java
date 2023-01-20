package compression;

import invertedIndex.LexiconStats;
import junit.framework.TestCase;
import org.apache.commons.lang3.ArrayUtils;
import org.mapdb.DataInput2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static utility.Utils.addByteArray;

public class CompressorTest extends TestCase {
    public BitSet unaryEncodeNumber(int x) {
        BitSet b = new BitSet(x);
        /*b.set(x-1);
        b.flip(0,x);*/
        b.set(1,x);
        return b;
    }

    public BitSet unaryEncode(int[] values) {
        //TODO: implement
        BitSet b = new BitSet();
        for(int x: values){

        }
        return b;
    }

    public int unaryDecodeNumber(BitSet b){
        return b.length();
    }

    public List<Integer> unaryDecode(byte[] b){
        List<Integer> numbers = new ArrayList<>();
        ArrayUtils.reverse(b); //forse non serve
        int n = 0;
        for(int i = 0; i < b.length; i++) {
            if(b[i]!= 0xFF && ((b[i] & 0x01) == 0)){
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

    public void testUnaryEncode() {
        BitSet b = unaryEncodeNumber(34);
        byte[] ba = b.toByteArray();
        System.out.println(b);
        ByteBuffer bf = ByteBuffer.allocate(b.length());
        bf.put(ba);
        bf.flip();
        //byte[] res = new byte[b.length()];
        //res = bf.get(res, 0, b.length()-1).array();
        byte[] res = ba;
        /*for(byte by: res){
            System.out.println(String.format("%8s", Integer.toBinaryString(by & 0xFF)).replace(' ', '0'));
        }*/
        BitSet out = BitSet.valueOf(res);
        //ArrayUtils.reverse(res);
        System.out.println(out);
        /*for(int i = 0; i < out.length(); i++){
            System.out.println(out.get(i));
        }*/
        System.out.println(unaryDecode(res));
    }

    //TODO: we need methods to convert byte arrays to Bitset!! Here are some examples found online:
    //FIRST EXAMPLE:
    public BitSet byteToBits(byte[] bytearray){
        BitSet returnValue = new BitSet(bytearray.length*8);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytearray);
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

    public static byte[] encode(int[] values) {

        // Determine the number of bytes needed to encode the values
        int numBytes = 0;
        for (int value : values) {
            numBytes += value >= 128 ? 4 : 1;
        }

        // Allocate a byte array to hold the encoded values
        byte[] encoded = new byte[numBytes];

        // Encode the values
        int index = 0;
        for (int value : values) {
            if (value < 128) {
                // Values less than 128 can be encoded in a single byte
                encoded[index++] = (byte) value;
            } else {
                // Values greater than or equal to 128 are encoded in four bytes
                encoded[index++] = (byte) (value >>> 24 | 0x80);
                encoded[index++] = (byte) (value >>> 16);
                encoded[index++] = (byte) (value >>> 8);
                encoded[index++] = (byte) value;
            }
        }

        return encoded;
    }

    public static int[] decode(byte[] encoded) {
        // Determine the number of values encoded in the byte array
        int numValues = 0;
        for (int i = 0; i < encoded.length; i++) {
            if ((encoded[i] & 0x80) == 0) {
                numValues++;
            }
        }

        // Allocate an array to hold the decoded values
        int[] values = new int[numValues];

        // Decode the values
        int index = 0;
        for (int i = 0; i < encoded.length; i++) {
            if ((encoded[i] & 0x80) == 0) {
                // Values less than 128 are encoded in a single byte
                values[index++] = encoded[i];
            } else {
                // Values greater than or equal to 128 are encoded in four bytes
                values[index++] = (encoded[i] & 0x7F) << 24 |
                        (encoded[i + 1] & 0xFF) << 16 |
                        (encoded[i + 2] & 0xFF) << 8 |
                        (encoded[i + 3] & 0xFF);
                i += 3;
            }
        }
        return values;
    }

    //TODO: decidere se per compressione e decompressione unaria ci serve una lista di numeri da (de)codificare
    // o se lavoriamo solo su singoli numeri

    //TODO: implement the following functions
    /*
        def VariableByteEncodeNumber(n):
    byte = []
    while True:
        byte.append(n % 128)
        if n < 128:
            break
        n //= 128
    byte[0] += 128
    return byte[::-1]
     */

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

    /*
        def VariableByteEncode(n_list):
    b = []
    for n in n_list:
        b.extend(VariableByteEncodeNumber(n))
    return b
     */
    public byte[] variableByteEncode(int[] n_list){
        byte[] byteStream = new byte[0];
        for(int n: n_list){
            byteStream = addByteArray(byteStream, variableByteEncodeNumber(n));
        }
        return byteStream;
    }
    /*
       def VariableByteDecode(bs):
    n_list = []
    n = 0
    for b in bs:
        if b < 128:
            n = 128 * n + b
        else:
            n = 128 * n + b - 128
            n_list.append(n)
            n = 0
    return n_list
     */

    public List<Integer> variableByteDecode(byte[] bs){
        List<Integer> numbers = new ArrayList<>();
        int n = 0;
        for(int i = 0; i < bs.length; i++){
            if((bs[i] & 0x80) < 128){
                n = 128*n + bs[i];
            }
            else{
                n = 128*n + (256+bs[i]) - 128;
                numbers.add(n);
                n = 0;
            }

        }
        return numbers;
    }

    public void testUnaryDecodeNumber() {
    }

    public void testUnaryDecode() {
        BitSet b1 = unaryEncodeNumber(4);
        BitSet b2 = unaryEncodeNumber(2);
        for(int i = 0; i < b1.length(); i++){
            System.out.println(b1.get(i));
        }
    }

    public void testVariableByteEncodeNumber() {
    }

    public void testVariableByteDecode1() {
        int[] arr = {33,45,675,21,132};
        System.out.println("INPUTS:");
        for(int i = 0; i < arr.length; i++){
            System.out.println(arr[i]);
        }
        byte[] b = variableByteEncode(arr);
        List<Integer> values = variableByteDecode(b);
        System.out.println("OUTPUTS:");
        for(int i = 0; i < values.size(); i++){
            System.out.println(values.get(i));
        }
    }

    public void testVariableByteDecode() {
        int[] arr = {33,45,675,21};
        byte[] b = encode(arr);
        int[] values = decode(b);
        for(int i = 0; i < values.length; i++){
            System.out.println(values[i]);
        }
    }

    public void testLex(){
        byte[] lexiconBytes;
        //take the document frequency
        byte[] dfBytes = ByteBuffer.allocate(4).putInt(23).array();
        //take the collection frequency
        byte[] cfBytes = ByteBuffer.allocate(8).putLong(22).array();
        //take list dim for both docids and tfs
        byte[] docBytes = ByteBuffer.allocate(4).putInt(21).array();
        byte[] tfBytes = ByteBuffer.allocate(4).putInt(25).array();
        //take the offset of docids
        byte[] offsetDocBytes = ByteBuffer.allocate(8).putLong(34).array();
        //take the offset of tfs
        byte[] offsetTfBytes = ByteBuffer.allocate(8).putLong(33).array();
        //concatenate all the byte arrays in order: key df cf docLen tfLen docOffset tfOffset
        lexiconBytes = dfBytes;
        lexiconBytes = addByteArray(lexiconBytes,cfBytes);
        lexiconBytes = addByteArray(lexiconBytes,docBytes);
        lexiconBytes = addByteArray(lexiconBytes,tfBytes);
        lexiconBytes = addByteArray(lexiconBytes,offsetDocBytes);
        lexiconBytes = addByteArray(lexiconBytes,offsetTfBytes);
        //write lexicon entry to disk
        ByteBuffer bufferLex = ByteBuffer.allocate(lexiconBytes.length);
        bufferLex.put(lexiconBytes);
        bufferLex.flip();
        LexiconStats l = new LexiconStats(bufferLex);
        System.out.println(l.getdF() + " " + l.getCf() + " " + l.getDocidsLen() + " " + l.getTfLen() + " " + l.getOffsetDocid() + " " + l.getOffsetTf());
    }
}