package preprocessing;

import junit.framework.TestCase;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class TokenizerTest extends TestCase {
    public void tokenize(String s) throws UnsupportedEncodingException {
        String[] input = s.split("\t");
        byte[] line = input[1].getBytes("UTF-8");
        String formattedLine = new String(line, "UTF-8");
        //remove all non-ASCII characters
        formattedLine = formattedLine.replaceAll("[^\\x00-\\x7F]", " ");
        // remove all the ASCII control
        //formattedLine = formattedLine.replaceAll("[^\\p{Cntrl}&&[^\r\n\t]]", "");
        // remove non-printable characters from Unicode
        formattedLine = formattedLine.replaceAll("\\p{C}", "");
        StringTokenizer st = new StringTokenizer(formattedLine);
        List<String> terms = new ArrayList<>();
        while (st.hasMoreTokens()) {
            terms.add(st.nextToken());
        }
        System.out.println(terms);
        System.out.println(input[0]);
    }


    public void testName() throws IOException {
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);

        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);
        String test = list.get(40);
        tokenize(test);
    }
}