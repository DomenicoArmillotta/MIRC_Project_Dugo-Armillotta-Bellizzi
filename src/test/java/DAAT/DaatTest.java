package DAAT;

import inverted_index.Compressor;
import inverted_index.Posting;
import junit.framework.TestCase;
import lexicon.Lexicon;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class DaatTest extends TestCase {
    public void testDaat() throws IOException {
        Daat daat = new Daat();
         /*String query = "1841";
        daat.daat(query);
        daat.daat("bile");
        daat.daat("acid");
        daat.daat("abdomin");
        daat.daat("academic");*/
        /*Scanner myObj = new Scanner(System.in);  // Create a Scanner object
        System.out.println("Enter <query rank>, where rank is 10 or 20");
        String input = myObj.nextLine();  // Read user input
        String query = input.split(" ")[0];
        String rank = input.split(" ")[1];
        if(!rank.equals("10") && !rank.equals("20")){
            System.out.println("Error! The rank is not correct");
            return;
        }
        int k = Integer.parseInt(rank);
        daat.daat(query, k);*/
        daat.disjunctiveDaat("A bile acid in the abdominal area of the stomach, U.S., bile acid", 10);
        daat.conjunctiveDaat("A bile acid in the abdominal area of the stomach, U.S., bile acid", 10);
        //daat.daat("What is the most beautiful city in the U.S. during summer in 1981", 20);
        daat.disjunctiveDaat("Most dangerous acid for the stomach and digestion", 20);
        daat.conjunctiveDaat("Most dangerous acid for the stomach and digestion", 20);
        //daat.daat("Capital of the US", 10);
        //daat.daat("Stomach acid", 10);*/

    }

    public void openListBin(String query_string) throws IOException {
        Lexicon lexicon = new Lexicon();
        Hashtable<String, Integer> htLexicon = lexicon.lexiconFromText("docs/lexicon_tot.bin");
        Hashtable<String, Integer> htLexiconL = lexicon.lexiconFromTextWithDocLen("docs/lexicon_tot.bin");
        Hashtable<String, Integer> htLexiconF = lexicon.lexiconFromTextFreq("docs/lexicon_tot.bin");
        Hashtable<String, Integer> htLexiconFL = lexicon.lexiconFromTextWithFreqLen("docs/lexicon_tot.bin");
        //String inputLex = "docs/lexicon_tot.txt";
        String inputDocids = "docs/inverted_index_docids.bin";
        String inputTfs = "docs/inverted_index_freq.bin";
        Set<String> globalTerms = new HashSet<>(htLexicon.keySet());
        TreeSet<String> sortedTerms = new TreeSet<>(globalTerms);
        Iterator<String> itTerms = sortedTerms.iterator();
        //System.out.println(query_string);
        LinkedList<Posting> postings_for_term = new LinkedList<>();
        int offset = 0;
        int docLen = 0;
        int freqLen = 0;
        int freqOffset = 0;
        //when is founded the term , a copy in data structure of inverted index is made
        while(itTerms.hasNext()){
            String term = itTerms.next();
            if(term.equals(query_string)) {
                offset = htLexicon.get(term);
                docLen = htLexiconL.get(term);
                freqOffset = htLexiconF.get(term);
                freqLen = htLexiconFL.get(term);
                System.out.println(term + " " + offset + " " + docLen + " " + freqOffset + " " + freqLen);
                break;
            }
        }
        Compressor c = new Compressor();
        c.decompressVariableByte(offset, docLen, inputDocids);
        c.decompressWithLF(freqOffset,freqLen,inputTfs);
        /*String docLine = (String) FileUtils.readLines(new File(inputDocids), "UTF-8").get(offset);
        postings_for_term = createPosting(docLine);
        return postings_for_term;*/

    }

    public void testBinDaat() throws IOException {
        openListBin("bile");
        openListBin("concern");
        openListBin("commun");
    }



    public LinkedList<Posting> createPosting(String docLine){
        System.out.println("NEW LIST");
        LinkedList<Posting> postings = new LinkedList<>();
        String[] docs_id = docLine.split(" ");
        for (int i=0;i<docs_id.length;i++){
            System.out.println("doc id : "+docs_id[i]);
        }
        return postings;
    }

    public LinkedList<Posting> openListBinFreq(String query_string) throws IOException {
        Lexicon lexicon = new Lexicon();
        Hashtable<String, Integer> htLexicon = lexicon.lexiconFromText("docs/lexicon_tot.bin");
        //String inputLex = "docs/lexicon_tot.txt";
        String inputDocids = "docs/inverted_index_docids.bin";
        Set<String> globalTerms = new HashSet<>(htLexicon.keySet());
        TreeSet<String> sortedTerms = new TreeSet<>(globalTerms);
        Iterator<String> itTerms = sortedTerms.iterator();
        //System.out.println(query_string);
        LinkedList<Posting> postings_for_term = new LinkedList<>();
        int offset = 0;
        //when is founded the term , a copy in data structure of inverted index is made
        while(itTerms.hasNext()){
            String term = itTerms.next();
            if(term.equals(query_string)) {
                offset = htLexicon.get(term);
                break;
            }
        }
        String docLine = (String) FileUtils.readLines(new File(inputDocids), "UTF-8").get(offset);
        postings_for_term = createPostingFreq(docLine);
        return postings_for_term;

    }

    public void testBinDaatFreq() throws IOException {
        openListBin("The bile is on the stomach acid abdomen");
    }



    public LinkedList<Posting> createPostingFreq(String docLine){
        System.out.println("NEW LIST");
        LinkedList<Posting> postings = new LinkedList<>();
        String[] docs_id = docLine.split(" ");
        for (int i=0;i<docs_id.length;i++){
            System.out.println("doc id : "+docs_id[i]);
        }
        return postings;
    }
}