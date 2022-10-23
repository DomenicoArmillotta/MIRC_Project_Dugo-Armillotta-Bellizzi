package preprocessing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

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
        // remove non-printable characters from Unicode
        formattedLine = formattedLine.replaceAll("\\p{C}", "");
        //remove punctuation
        formattedLine = formattedLine.replaceAll("\\p{Punct}", "");
        //replace the country codes with the country name
        formattedLine = replaceCountryCodes(formattedLine);
        //lower case
        formattedLine = formattedLine.toLowerCase();
        return formattedLine;

    }

    private String replaceCountryCodes(String doc) throws IOException {
        File file_nations = new File("docs/nations.txt");
        List<String> list_codes = new ArrayList<>();
        // to not read all the documents in memory we use a LineIterator to improve memory efficency
        LineIterator it = FileUtils.lineIterator(file_nations, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                list_codes.add(line);
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        for(String nation: list_codes){
            String[] inputs = nation.split(","); //the line is in the format Country_name, Country_code
            String code = inputs[1]; //country code
            String name = inputs[0]; //country name
            if(doc.contains(" " +code + " ") || doc.contains(code + " ") || doc.contains(" " + code)){
                doc = doc.replaceAll(code, name);
            }
        }
        return doc;
    }
}
