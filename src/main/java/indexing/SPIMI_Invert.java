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
        int size = (int) Math.ceil(lines/n); //size of each block of lines
        BufferedReader reader = Files.newBufferedReader(p, StandardCharsets.UTF_8);
        String textfileRow = null;
        List<String> fileLines = new ArrayList<>();
        int CHUNKSIZE = size; //the size of each chunk is the size of the blocks
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

    //TODO: complete the algorithm!
    //we have for each call a block of the file; for each block we create a inverted index with his dictionary and apply the alghorithm;
    //at the end we use the inverted index method to write to the disk
    public void spimi_invert(List<String> fileBlock, int n){
        Inverted_index index = new Inverted_index();//constructor: initializes the dictionary and the output file
        for(String doc : fileBlock){ //each row is a doc!
            //read the terms and generate postings
            
            //write postings
            //at the end of the block we have to sort the posting lists in lexicographic order
            //then we merge the posting lists
            //and write to the output file
        }
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
