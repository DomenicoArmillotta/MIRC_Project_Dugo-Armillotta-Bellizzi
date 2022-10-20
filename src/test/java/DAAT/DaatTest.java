package DAAT;

import indexing.SPIMI_Invert;
import junit.framework.TestCase;

import java.io.IOException;

public class DaatTest extends TestCase {
    public void testDaat() throws IOException {
        Daat daat = new Daat();
        daat.daat("1841");
    }
}