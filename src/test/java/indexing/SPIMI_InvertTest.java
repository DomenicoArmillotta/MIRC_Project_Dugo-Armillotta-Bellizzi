package indexing;

import junit.framework.TestCase;

import java.io.IOException;

public class SPIMI_InvertTest extends TestCase {
    public void testSpimi() throws IOException {
        SPIMI_Invert si = new SPIMI_Invert();
        String path = "docs/collection_test.tsv";
        si.spimi_invert_block(path,10);
    }
}