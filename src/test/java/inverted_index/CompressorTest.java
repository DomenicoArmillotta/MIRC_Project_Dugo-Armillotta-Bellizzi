package inverted_index;

import junit.framework.TestCase;

public class CompressorTest extends TestCase {

    public int getK(int x){
        return (int) Math.ceil(Math.log(x+1)/Math.log(2));
    }

    public String bin(int x){
        return Integer.toBinaryString(x);
    }

    public String unary(int x){
        String bitString = new String(new char[x-1]).replace("\0", "1");
        bitString += "0";
        return bitString;
    }

    public int decodeUnary(String bitString){
        int x = 0;
        String[] ones = bitString.split("0");
        x = ones[0].length() + 1;
        return x;
    }

    public String gamma(int x){
        String bitString = unary(getK(x)) + "." + bin(x).substring(1);
        return bitString;
    }

    public int decodeGammaDelta(String bitString){
        int index = bitString.indexOf(".");
        String binary = "1" + bitString.substring(index+1);
        int x = Integer.parseInt(binary,2);
        return x;
    }

    public String delta(int x){
        String t_gamma = gamma(getK(x)).replace(".", "");
        String bitString = t_gamma + "." + bin(x).substring(1);
        return bitString;
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
            if (end != mod) {
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

    public int decodeVariableByte(String bitString){
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



    public void testCode(){
        assertTrue(delta(4).equals("101.00"));
        assertTrue(delta(8).equals("11000.000"));
        assertTrue(gamma(5).equals("110.01"));
        assertTrue(variable_byte(67822).equals("000001001001000111101110"));
        assertTrue(decodeUnary("111111110") == 9);
        decodeGammaDelta("11000.000");
        assertTrue(decodeGammaDelta("11000.000") == 8);
        assertTrue(decodeGammaDelta("110.01") == 5);
        assertTrue(decodeVariableByte("000001001001000111101110") == 67822);
    }
}