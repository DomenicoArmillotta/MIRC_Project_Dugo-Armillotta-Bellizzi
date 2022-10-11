package document_index;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Hashtable;
import java.util.List;

public class Document_indexTest extends TestCase {

    public void testName() throws IOException {
        Document_index document_index = new Document_index();
        String path = "docs/collection_test.tsv";
        Hashtable<Integer , List<Integer>> ht = new Hashtable<>();
        ht = document_index.create_document_index(path);
        System.out.println(ht);
        document_index.text_from_document_index(ht);
        String save_path = "docs/document_index_test.tsv";
        Hashtable<Integer , List<Integer>> ht_test = new Hashtable<>();
        ht_test = document_index.document_index_from_text(save_path);
        System.out.println(ht_test);
        assertTrue(ht.equals(ht_test));
    }
}