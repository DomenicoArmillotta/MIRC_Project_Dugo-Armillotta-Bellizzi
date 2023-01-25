package compression;

import org.apache.commons.lang3.ArrayUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static utility.Utils.addByteArray;

public class Compressor {


    /**
     * compression used in the compression of tf
     * @param x int to encode
     * @return
     */
    public byte[] unaryEncode(int x) {
        if(x==1){
            byte[] res = new byte[1];
            return res;
        }
        BitSet b = new BitSet(x);
        b.set(1,x);
        return b.toByteArray();
    }


    /**
     *  used to decode the compressed unary , used in the decompression of tf compressed
     * @param b byte array to decompress
     * @return
     */
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
