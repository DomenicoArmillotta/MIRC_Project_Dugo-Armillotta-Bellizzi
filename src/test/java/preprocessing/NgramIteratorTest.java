package preprocessing;

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class NgramIteratorTest extends TestCase {
    private final String str;
    private final int n;
    int pos = 0;
    public NgramIteratorTest(int n, String str) {
        this.n = n;
        this.str = str;
    }
    public boolean hasNext() {
        return pos < str.length() - n + 1;
    }
    public String next() {
        return str.substring(pos, pos++ + n);
    }
    public static List<String> ngrams(int n, String str) {
        List<String> ngrams = new ArrayList<String>();
        String[] words = str.split(" ");
        for (int i = 0; i < words.length - n + 1; i++)
            ngrams.add(concat(words, i, i+n));
        return ngrams;
    }

    public static String concat(String[] words, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < end; i++)
            sb.append((i > start ? " " : "") + words[i]);
        return sb.toString();
    }

    public static void main( String args[] ) throws IOException {
        String s = "abcdef";
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);

        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);
        String test = list.get(89);
        //new NgramIterator(3, test.split("\t")[1]).forEachRemaining(System.out::println);
        System.out.println(ngrams(2,test.split("\t")[1]));
        System.out.println(ngrams(3,test.split("\t")[1]));
        System.out.println(ngrams(4,test.split("\t")[1]));
        System.out.println(ngrams(5,test.split("\t")[1]));
    }
}