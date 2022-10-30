package document_index;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.Hashtable;

public class Document_indexTest extends TestCase {

    public void testName() throws IOException {
        DocumentIndex document_index = new DocumentIndex();
        String path = "docs/collection_test.tsv";
        Hashtable<Integer , Integer> ht = new Hashtable<>();
        ht = document_index.createDocumentIndex(path);
        System.out.println(ht);
        document_index.textFromDocumentIndex(ht);
        String save_path = "docs/document_index.txt";
        Hashtable<Integer , Integer> ht_test = new Hashtable<>();
        ht_test = document_index.documentIndexFromText(save_path);
        System.out.println(ht_test);
        assertTrue(ht.equals(ht_test));
    }
}