package preprocessing;

import junit.framework.TestCase;
import smile.nlp.tokenizer.PennTreebankTokenizer;
import weka.core.tokenizers.WordTokenizer;

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

    public void bytePairEncoder(String s){
        //choose the two symbols that are most frequently adjacent in the training corpus (A, B)

        //add a new merged symbol AB to the vocabulary

        //replace every adjacent A B in the corpus with AB
    }
    /**
     * Penn Tree Bank tokenizer:
     * A word tokenizer that tokenizes English sentences using the conventions
     * used by the Penn Treebank. Most punctuation is split from adjoining words.
     * Verb contractions and the Anglo-Saxon genitive of nouns are split into their
     * component morphemes, and each morpheme is tagged separately. Examples
     *
     * This tokenizer assumes that the text has already been segmented into
     * sentences. Any periods -- apart from those at the end of a string or before
     * newline -- are assumed to be part of the word they are attached to (e.g. for
     * abbreviations, etc), and are not separately tokenized.
     *
     */

    public void testName() throws IOException {
        String path = "docs/collection_test.tsv";
        File file = new File("docs/collection_test.tsv");
        Path p = Paths.get(path);
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);

        List<String> list = Files.readAllLines(p, StandardCharsets.UTF_8);
        String test = list.get(15);
        tokenize(test);
        PennTreebankTokenizer wordTokenizer = PennTreebankTokenizer.getInstance();
        String[] inputs = test.split("\t");
        String s = inputs[1];
        String[] outputs = wordTokenizer.split(s);
        for(String elem : outputs){
            System.out.println(elem);
        }
    }
}