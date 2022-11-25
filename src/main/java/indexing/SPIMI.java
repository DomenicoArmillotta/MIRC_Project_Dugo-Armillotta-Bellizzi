package indexing;


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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SPIMI {

    private DB db;
    private HTreeMap<String, Integer> documentIndex;

    //creazione dei blocchi usando la ram
    public void spimiInvertBlockMapped(String read_path) throws IOException {
        int n_block = 0;
        db = DBMaker.fileDB("docs/testDB.db").make();
        documentIndex = db
                .hashMap("documentIndex")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.INTEGER)
                .createOrOpen();
        File input_file = new File(read_path);
        LineIterator it = FileUtils.lineIterator(input_file, "UTF-8");
        int index_block = 0;
        try {
            //create chunk of data , splitting in n different block
            while (it.hasNext()){  //--> its the ram of jvm
                List<String> listDoc = new ArrayList<>();
                int i = 0;
                while (it.hasNext() && (Runtime.getRuntime().totalMemory()*0.80 <= Runtime.getRuntime().freeMemory())) {
                    String line = it.nextLine();
                    listDoc.add(line);
                    i++;
                }
                //we elaborate one block at time , so we call the function to create inverted index for the block
                spimiInvertMapped(listDoc, index_block);
                n_block++;
                index_block++;
            }

        } finally {
            LineIterator.closeQuietly(it);
        }
        db.commit();
        db.close();
        //FARE MERGE
    }

    public void spimiInvertMapped(List<String> fileBlock, int n) throws IOException {

    }

    private void mergeBlocks(int n){

    }




    public void spimiInvert(List<String> fileBlock, int n) throws IOException {

    }






    //fast to count file line --> 5milion in 4/5 s
    public static long countLineFast(String fileName_path) {

        long lines = 0;

        try (InputStream is = new BufferedInputStream(new FileInputStream(fileName_path))) {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean endsWithoutNewLine = false;
            while ((readChars = is.read(c)) != -1) {
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n')
                        ++count;
                }
                endsWithoutNewLine = (c[readChars - 1] != '\n');
            }
            if (endsWithoutNewLine) {
                ++count;
            }
            lines = count;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lines;
    }


}




