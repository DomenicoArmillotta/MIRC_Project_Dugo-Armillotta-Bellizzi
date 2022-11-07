package inverted_index;

import java.util.List;

public class Compressor {

    private int p_docs;
    private int p_freq;
    private int c_docs;
    private int c_freq;
    private int n_postings;

    public int getK(int x){
        return (int) Math.ceil(Math.log(x+1)/Math.log(2));
    }

    public String bin(int x){
        return Integer.toBinaryString(x);
    }

    public String unary(int x){
        String bitString = new String(new char[x]).replace("\0", "1");
        bitString += "0";
        return bitString;
    }
    public int decodeUnary(String bitString){
        int x = 0;
        String[] ones = bitString.split("0");
        x = ones[0].length() ;
        return x;
    }

    public String gamma(int x){
        String bitString = unary(getK(x)) + "." + bin(x).substring(1);
        return bitString;
    }

    public String delta(int x){
        String t_gamma = gamma(getK(x)).replace(".", "");
        String bitString = t_gamma + "." + bin(x).substring(1);
        return bitString;
    }

    public int decodeGammaDelta(String bitString){
        int index = bitString.indexOf(".");
        String binary = "1" + bitString.substring(index+1);
        int x = Integer.parseInt(binary,2);
        return x;
    }

    public String variable_byte(int x){
        String bitString = "";
        String bin = bin(x);
        int l = bin.length();
        int mod = l%7;
        bitString+= new String(new char[mod+2]).replace("\0", "0");
        int start = 0;
        int end = mod;
        while(end<=l) {
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
        //System.out.println(bitString);
        return bitString;
    }

    public int decodeVariableByteOld(String bitString){
        String first = bitString.substring(0,8);
        int i = first.indexOf("1");
        String bin = "";
        bin += first.substring(i);
        int cont = 8;
        int l = bitString.length();
        while(cont<l){
            String temp = bitString.substring(cont,cont+8);
            i = temp.indexOf("1") + 1;
            bin += temp.substring(i);
            cont+=8;
        }
        //System.out.println(bin);
        return Integer.parseInt(bin,2);
    }

    public int decodeVariableByte(String bitString){
        String bin="";
        int i = 0;
        i = bitString.indexOf("1");
        if(bitString.length()%8!=0) {
            bin += bitString.substring(i, bitString.length() % 8);
        }
        else {
            bin += bitString.substring(i, 8);
        }
        int cont = 8;
        int l = bitString.length();
        while(cont<l){
            String temp = bitString.substring(cont,cont+8);
            i = temp.indexOf("1") + 1;
            bin += temp.substring(i);
            cont+=8;
        }
        //System.out.println(bin);
        return Integer.parseInt(bin,2);
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
