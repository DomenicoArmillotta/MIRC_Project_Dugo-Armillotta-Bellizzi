package indexing;

import compression.Compressor;
import inverted_index.Posting;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import preprocessing.PreprocessDoc;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class SPIMI_InvertTest extends TestCase {


}