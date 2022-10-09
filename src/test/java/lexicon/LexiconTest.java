package lexicon;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Hashtable;

public class LexiconTest extends TestCase {
    public void testName() throws IOException {
        Lexicon lexicon = new Lexicon();
        String path = "docs/collection_test.tsv";
        Hashtable<String ,Integer> ht = new Hashtable<>();
        ht = lexicon.create_lexicon(path);
        System.out.println(ht);
    }
}