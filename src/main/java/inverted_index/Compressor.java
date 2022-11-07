package inverted_index;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class Compressor {

    private int p_docs;
    private int p_freq;
    private int c_docs;
    private int c_freq;
    private int n_postings;

    public int getK(int x) {
        return (int) Math.ceil(Math.log(x + 1) / Math.log(2));
    }

    public String bin(int x) {
        return Integer.toBinaryString(x);
    }

    public String unary(int x) {
        String bitString = new String(new char[x]).replace("\0", "1");
        bitString += "0";
        return bitString;
    }

    public int decodeUnary(String bitString) {
        int x = 0;
        String[] ones = bitString.split("0");
        x = ones[0].length();
        return x;
    }

    public String gamma(int x) {
        String bitString = unary(getK(x)) + "." + bin(x).substring(1);
        return bitString;
    }

    public String delta(int x) {
        String t_gamma = gamma(getK(x)).replace(".", "");
        String bitString = t_gamma + "." + bin(x).substring(1);
        return bitString;
    }

    public int decodeGammaDelta(String bitString) {
        int index = bitString.indexOf(".");
        String binary = "1" + bitString.substring(index + 1);
        int x = Integer.parseInt(binary, 2);
        return x;
    }

    public String variable_byte(int x) {
        String bitString = "";
        String bin = bin(x);
        int l = bin.length();
        int mod = l % 7;
        bitString += new String(new char[mod + 2]).replace("\0", "0");
        int start = 0;
        int end = mod;
        while (end <= l) {
            String temp = "";
            temp = bin.substring(start, end);
            if (end != mod) { //if it's not the first block, we put the leading 1 to the string
                bitString += "1";
                start += 7;
            } else {
                start += mod;
            }
            bitString += temp;
            end += 7;
        }
        System.out.println(bitString);
        return bitString;
    }

    public int decodeVariableByteOld(String bitString){
        String first = bitString.substring(0,8);
        int i = first.indexOf("1");
        String bin = "";
        bin += first.substring(i);
        int cont = 8;
        int l = bitString.length();
        while (cont < l) {
            String temp = bitString.substring(cont, cont + 8);
            i = temp.indexOf("1") + 1;
            bin += temp.substring(i);
            cont += 8;
        }
        //System.out.println(bin);
        return Integer.parseInt(bin, 2);
    }

    public int decodeVariableByte(String bitString){
        String bin="";
        int i = 0;
        i = bitString.indexOf("1");
        bin += bitString.substring(i, bitString.length() % 8);
        int cont = 8-bitString.length()%8;
        int l = bitString.length();
        while(cont<l){
            String temp = "";
            if(bitString.length() < cont+8){
                temp = bitString.substring(cont,l);
            }
            else{
                temp = bitString.substring(cont,cont+8);
            }
            i = temp.indexOf("1") + 1;
            bin += temp.substring(i);
            cont+=8;
        }
        //System.out.println(bin);
        return Integer.parseInt(bin,2);
    }

    public String stringCompressionWithLF(String x) throws IOException {
        Compressor compressor = new Compressor();
        RandomAccessFile stream = new RandomAccessFile("docs/inverted_index_freq.bin", "rw");
        FileChannel channel = stream.getChannel();
        String bitString = compressor.unary(Integer.parseInt(x));
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        ByteBuffer bufferValue = ByteBuffer.allocate(ba.length);
        bufferValue.put(ba);
        bufferValue.flip();
        channel.write(bufferValue);
        ByteBuffer bufferSpace = ByteBuffer.allocate(ba.length);
        bufferSpace.put("\n".getBytes());
        bufferSpace.flip();
        channel.write(bufferSpace);
        stream.close();
        return bitString;
    }


    public int decompressWithLF(String bitString) throws IOException {
        Compressor compressor = new Compressor();
        RandomAccessFile stream = new RandomAccessFile("docs/inverted_index_freq.bin", "r");
        FileChannel channel = stream.getChannel();
        //set the buffer size
        int bufferSize = 1;
        int res = 0;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        String prev = "";
        // read the data from filechannel
        while (channel.read(buffer) != -1) {
            byte[] input = (buffer.array());
            BigInteger one = new BigInteger(input);
            if (one.compareTo(BigInteger.ZERO) < 0)
                one = one.add(BigInteger.ONE.shiftLeft(8));
            //--> unario
            String strResult = one.toString(2);
            if (strResult.equals("1010")) {
                System.out.println("end line!");
                break;
            }
            if (strResult.indexOf("0") == -1 || strResult.equals("0")) {
                System.out.println("not zero: " + strResult);
                prev += strResult;
            } else {
                if (prev != "") {
                    System.out.println("previous: " + prev);
                    prev += strResult;
                    System.out.println("ending string: " + strResult);
                    System.out.println(prev);
                    if (prev.startsWith("0")) {
                        prev = prev.substring(prev.indexOf("0") + 1);
                    }
                    System.out.println(compressor.decodeUnary(prev));
                    prev = "";
                } else {
                    System.out.println("string: " + strResult);
                    System.out.println(compressor.decodeUnary(strResult));
                }
            }
            res = compressor.decodeUnary(strResult);
            buffer.clear();
        }
        // clode both channel and file
        channel.close();
        stream.close();
        return res;
    }




    public int computeDocsProb(int x){
        p_docs =  c_docs/n_postings;
        return p_docs;
    }

    public int computeFreqProb(int x){
        p_freq =  c_freq/n_postings;
        return p_freq;
    }

    public int optimalProb(int codelength){
        return 2^(0-Math.abs(codelength));
    }

    public int getP_docs() {
        return p_docs;
    }

    public void setP_docs(int p_docs) {
        this.p_docs = p_docs;
    }

    public int getP_freq() {
        return p_freq;
    }

    public void setP_freq(int p_freq) {
        this.p_freq = p_freq;
    }


    public int getC_docs() {
        return c_docs;
    }

    public void setC_docs(int c_docs) {
        this.c_docs = c_docs;
    }


    public int getC_freq() {
        return c_freq;
    }

    public void setC_freq(int c_freq) {
        this.c_freq = c_freq;
    }

    public int getN_postings() {
        return n_postings;
    }

    public void setN_postings(int n_postings) {
        this.n_postings = n_postings;
    }

}
