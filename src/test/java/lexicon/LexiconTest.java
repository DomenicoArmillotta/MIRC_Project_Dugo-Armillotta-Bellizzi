package lexicon;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Hashtable;

public class LexiconTest extends TestCase {
    public void testName() throws IOException {
        Lexicon lexicon = new Lexicon();
        String path = "docs/collection_test.tsv";
        Hashtable<String ,Integer> ht_original = new Hashtable<>();
        Hashtable<String ,Integer> ht_text = new Hashtable<>();
        ht_original = lexicon.create_lexicon(path);
        System.out.println(ht_original);
        lexicon.text_from_lexicon(ht_original);
        String save_path = "docs/lexicon_test.tsv";
        ht_text = lexicon.lexicon_from_text(save_path);
        System.out.println(ht_text);
        assertTrue(ht_original.equals(ht_text));


    }
}