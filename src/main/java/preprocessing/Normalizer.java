package preprocessing;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Normalizer {
    public String normalize(String s) throws UnsupportedEncodingException {
        //String[] input = s.split("\t");
        //byte[] line = input[1].getBytes("UTF-8");
        //String formattedLine = new String(line, "UTF-8");
        //remove all non-ASCII characters
        String formattedLine = s.replaceAll("[^\\x00-\\x7F]", " ");
        // remove all the ASCII control
        //formattedLine = formattedLine.replaceAll("[^\\p{Cntrl}&&[^\r\n\t]]", "");
        // remove non-printable characters from Unicode
        formattedLine = formattedLine.replaceAll("\\p{C}", "");
        //remove punctuation
        formattedLine = formattedLine.replaceAll("\\p{Punct}", "");
        return formattedLine;

    }
}
