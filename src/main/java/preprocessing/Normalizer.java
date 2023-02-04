package preprocessing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * used in the normalization phase in the pre-processing step executed for each document in the collection
 */
public class Normalizer {
    /**
     * first step of preprocessing, regexes are used for:
     * - remove non-ascii characters
     * - remove non-printable characters
     * - remove punctuation
     * - replace country code with the country name
     * then the lower case is made
     * @param s string in input
     * @return formattedLine string in output
     * @throws UnsupportedEncodingException
     */
    public String normalize(String s) throws IOException {
        //remove all non-ASCII characters
        String formattedLine = s.replaceAll("[^\\x00-\\x7F]", " ");
        //remove malformed characters
        formattedLine = formattedLine.replaceAll("[^a-zA-Z0-9/?:().,'+/-]", " ");
        // remove non-printable characters from Unicode
        formattedLine = formattedLine.replaceAll("\\p{C}", " ");
        //replace urls
        String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
        formattedLine = formattedLine.replaceAll(urlPattern, " ");
        //remove punctuation
        formattedLine = formattedLine.replaceAll("\\p{Punct}", " ");
        //replace digits
        formattedLine = formattedLine.replaceAll("\\d", " ");
        //replace all single characters
        formattedLine = formattedLine.replaceAll("(\\s+.(?=\\s))", " ");
        //collapse multiple spaces
        formattedLine = formattedLine.replaceAll("\\s+", " ");
        //lower case
        formattedLine = formattedLine.toLowerCase();

        return formattedLine;
    }

}
