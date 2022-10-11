package indexing;

import inverted_index.Inverted_index;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class SPIMI_Invert {
    public void spimi_invert_block(String path, int n) throws IOException {
        Preprocess_doc preprocessing = new Preprocess_doc();
        File file = new File(path);
        Path p = Paths.get(path);
        List<String> list_doc = new ArrayList<>();
        long lines = getFileLines(path);
        int CHUNKSIZE = (int) Math.ceil(lines/n); //size of each block of lines
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        String textfileRow = null;
        List<String> fileLines = new ArrayList<>();
        int lineIndex = 0;
        int nChunk = 0;
        while (lineIndex < lines) //for each chunk we read all the lines and apply SPIMI
        {
            fileLines.add(textfileRow);
            int chunkEnd = lineIndex + CHUNKSIZE;

            if (chunkEnd >= fileLines.size())
            {
                chunkEnd = fileLines.size();
            }
            //for each block of files, I call SPIMI
            List<String> mySubList = fileLines.subList(lineIndex, chunkEnd);
            spimi_invert(mySubList, nChunk);
            nChunk++;
            lineIndex = chunkEnd;
        }
        //merge results of each block into one file: for each chunk (0 to nChunk-1) with
    }

    //TODO 10/10/2022: complete the algorithm!
    //we have for each call a block of the file; for each block we create a inverted index with his dictionary and apply the alghorithm;
    //at the end we use the inverted index method to write to the disk
    public void spimi_invert(List<String> fileBlock, int n) throws IOException {
        Inverted_index index = new Inverted_index();//constructor: initializes the dictionary and the output file
        Preprocess_doc preprocessing = new Preprocess_doc();
        for(String doc : fileBlock){ //each row is a doc!
            String[] parts = doc.split("\t");
            int doc_id = Integer.parseInt(parts[0]);
            String doc_corpus = parts[1];
            List<String> pro_doc = new ArrayList<>();
            pro_doc = preprocessing.preprocess_doc_optimized(doc_corpus);
            //read the terms and generate postings
            //write postings
            for(String term : pro_doc){
                index.addToDict(term);
                index.addPosting(term, doc_id, index.getDict().get(term));
            }
        }
        //at the end of the block we have to sort the posting lists in lexicographic order
        index.sortPosting();
        //then we merge the posting lists
        //and write to the output file
    }

    private static long getFileLines(String path){
        long result = 0;
        try{
            result = Files.lines(Paths.get(path)).count();

        }
        catch(IOException e){
            e.printStackTrace();
        }
        return result;
    }
}
