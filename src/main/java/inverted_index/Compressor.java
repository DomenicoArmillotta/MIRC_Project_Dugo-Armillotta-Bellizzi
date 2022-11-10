package inverted_index;

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
        if(bitString.equals("0")) return 0;
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

    public String variableByteNew(int x){
        String bitString = "";
        String bin = bin(x);
        int l = bin.length();
        int mod = l%7;
        /*if(mod!=0) {
            bitString += new String(new char[7 - mod + 1]).replace("\0", "0");
        }*/
        //place the leading zeros to identify the last byte of the encoding
        bitString += new String(new char[7 - mod + 1]).replace("\0", "0");
        int start = 0;
        int end = mod;
        while(end<=l) {
            String temp = "";
            temp = bin.substring(start, end);
            if (end != mod) {
                temp = "1" + temp;
                start += 7;
                bitString = temp + bitString;
            } else {
                start += mod;
                bitString+=temp;
            }
            end += 7;
        }
        return bitString;
    }

    public int decodeVariableByteNew(String bitString){
        if(bitString.equals("0")) return 0;
        String bin="";
        int i = 0;
        int cont = 0;
        int l = bitString.length();
        while(cont<l){
            String temp="";
            int end = cont+8 > l? l : cont+8;
            temp = bitString.substring(cont,end);
            if(temp.equals("0")){
                continue;
            }
            else if(temp.substring(temp.indexOf("1")).length() != 8){
                i = temp.indexOf("1");
            }
            else {
                i = temp.indexOf("1") + 1;
            }
            bin = temp.substring(i) + bin;
            cont+=8;
        }
        //System.out.println(bin);
        return Integer.parseInt(bin,2);
    }

    public byte[] stringCompressionWithLF(int x) throws IOException {
        String bitString = unary(x);
        //System.out.println(x + " " + bitString);
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        return ba;
    }

    public byte[] stringCompressionWithVariableByte(int doc){
        String bitString = variableByteNew(doc);
        //System.out.println(bitString);
        byte[] ba = new BigInteger(bitString, 2).toByteArray();
        if(ba[0]==0 && ba.length>1){ //because toByteArray() adds one bit 0 for the sign, so we have one extra byte
            ba = Arrays.copyOfRange(ba, 1, ba.length);
        }
        return ba;

    }

    public void decompressWithLF(int offset, int end, String path) throws IOException {
        RandomAccessFile stream = new RandomAccessFile(path, "r");
        //FileChannel channel = stream.getChannel();
        Path filepath = Paths.get(path);
        SeekableByteChannel channel = Files.newByteChannel(filepath, StandardOpenOption.READ);
        channel = channel.position(offset);
        //set the buffer size
        int bufferSize = 1;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        String prev = "";
        int cont = 0;
        int n = 0;
        // read the data from filechannel
        while (channel.read(buffer) != -1) {
            byte[] input = (buffer.array());
            /*if(cont!=offset){
                cont++;
                buffer.clear();
                continue;
            }*/
            BigInteger one = new BigInteger(input);
            if (one.compareTo(BigInteger.ZERO) < 0)
                one = one.add(BigInteger.ONE.shiftLeft(8));
            //--> unario
            String strResult = one.toString(2);
            /*if (strResult.equals("1010")) {
                //System.out.println("end line!");
                break;
            }*/
            //check if the string is all ones
            //if (strResult.indexOf("0") == -1 || strResult.equals("0")) {
            if (strResult.indexOf("0") == -1) {
                System.out.println("not zero: " + strResult);
                prev += strResult;
                n++;
            }
            else {
                if (prev != "") {
                    //System.out.println("previous: " + prev);
                    prev += strResult;
                    //System.out.println("ending string: " + strResult);
                    //System.out.println(prev);
                    if (prev.startsWith("0")) {
                        prev = prev.substring(prev.indexOf("0") + 1);
                    }
                    System.out.println("RESULT: " + decodeUnary(prev));
                    prev = "";
                    n++;
                } else {
                    System.out.println("RESULT: " + decodeUnary(strResult));
                    n++;
                }
            }
            buffer.clear();
            if(n==end) break;
        }
        // close both channel and file
        channel.close();
        stream.close();
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
        //channel = channel.truncate(end);
        String prev = "";
        int nextValue = 0;
        int cont = 0;
        int n = 0;
        // read the data from filechannel
        while (channel.read(buffer) != -1) {
            byte[] input = (buffer.array());
            /*if(cont!=offset){
                cont++;
                buffer.clear();
                continue;
            }*/
            BigInteger one = new BigInteger(input);
            if (one.compareTo(BigInteger.ZERO) < 0)
                one = one.add(BigInteger.ONE.shiftLeft(8));
            buffer.clear();
            String strResult = one.toString(2);
            //System.out.println(strResult);
            if(strResult.length() == 8){
                prev+= strResult;
                nextValue++;
                n++;
            }
            else if(strResult.equals("0") && nextValue > 0){
                if(!prev.equals("")){
                    System.out.println("RESULT: " + decodeVariableByteNew(prev));
                    nextValue = decodeVariableByteNew(prev) + 1;
                    prev ="";
                    n++;
                    if(n==end) break;
                }
            }
            else{
                if(prev!=""){
                    prev+=strResult;
                    if(prev.startsWith("0")){
                        prev = prev.substring(prev.indexOf("0")+1);
                    }
                    System.out.println("RESULT: " + decodeVariableByteNew(prev));
                    prev = "";
                    nextValue = decodeVariableByte(strResult) + 1;
                }
                else{
                    System.out.println("RESULT: " + decodeVariableByteNew(strResult));
                    nextValue = decodeVariableByte(strResult) + 1;
                }
                n++;
                if(n==end) break;
            }
        }
        // close both channel and file
        channel.close();
        fileinput.close();
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
