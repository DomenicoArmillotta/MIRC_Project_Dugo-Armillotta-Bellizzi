package preprocessing;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

public class Normalizer {
    /**
     * first step of preprocessing, regexes are used for:
     * - remove non-ascii characters
     * - remove non-printable characters
     * -remove punctuation
     * then the lower case is made
     * @param s string in input
     * @return formattedLine string in output
     * @throws UnsupportedEncodingException
     */
    public String normalize(String s) throws UnsupportedEncodingException {
        //remove all non-ASCII characters
        String formattedLine = s.replaceAll("[^\\x00-\\x7F]", " ");
        // remove non-printable characters from Unicode
        formattedLine = formattedLine.replaceAll("\\p{C}", "");
        //remove punctuation
        formattedLine = formattedLine.replaceAll("\\p{Punct}", "");
        //lower case
        formattedLine = formattedLine.toLowerCase();
        return formattedLine;

    }
}
