package DAAT;

import inverted_index.Posting;
import preprocessing.Preprocess_doc;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
//voglio copiare le posting list dei termini della query all'interno di una struttura dati
//quindi avro solo un inverted index dei termini della quey

//non ci sono metodi per saltare la lettura dell n righe in un file
public class Daat {
    public void daat(String query_string) throws IOException {
        HashMap<String, List<Posting>> inverted_index_query = new HashMap<>();
        inverted_index_query = create_inverted_query(query_string);
        int[][] rank = new int[10][2];



    }







    public HashMap<String, List<Posting>> create_inverted_query (String query_string) throws IOException {
        Preprocess_doc preprocessing = new Preprocess_doc();
        //preprocessing of query
        List<String> pro_query = new ArrayList<>();
        pro_query = preprocessing.preprocess_doc_optimized(query_string);
        String inputLex = "docs/lexicon_tot.txt";
        String inputDocids = "docs/inverted_index_docids.txt";
        String inputFreqs = "docs/inverted_index_freq.txt";
        String inputPos = "docs/inverted_index_pos.txt";
        //created buffer to read file
        BufferedReader itLex  = Files.newBufferedReader(Paths.get(inputLex), StandardCharsets.UTF_8);
        BufferedReader itId  = Files.newBufferedReader(Paths.get(inputDocids), StandardCharsets.UTF_8);
        BufferedReader itTf  = Files.newBufferedReader(Paths.get(inputFreqs), StandardCharsets.UTF_8);
        BufferedReader itPos  = Files.newBufferedReader(Paths.get(inputPos), StandardCharsets.UTF_8);
        String lexLine = null;
        //in this structure we have all posting of term of query
        HashMap<String, List<Posting>> inverted_index_query = new HashMap<>();
        ArrayList<Posting> postings = new ArrayList<>();
        //iterate through all term of query
        for(String term : pro_query ){
            List<Posting> postings_for_term = new ArrayList<>();
            int offset = 0;
            //when is founded the term , a copy in data structure of inverted index is made
            while((lexLine = itLex.readLine()) != null){
                String[] inputs = lexLine.split(" ");
                if(inputs[0].equals(term)){
                    offset = Integer.parseInt(inputs[1]); //--> this is the offset of term , we can use to retrive other info
                    System.out.println(offset);
                }
            }
            //go to the offset to retrieve all info : doc_id , tf , pos
            int i=0;
            while(i<(offset-1)){
                itId.readLine();
                itTf.readLine();
                itPos.readLine();
                i++;
            }
            String docLine = null;
            String posLine = null;
            String tfLine = null;
            docLine = itId.readLine();
            docLine = docLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            posLine = itPos.readLine();
            posLine = posLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            tfLine = itTf.readLine();
            tfLine = tfLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            postings_for_term = createPosting(docLine,tfLine,posLine);
            inverted_index_query.put(term,postings_for_term);

        }

        //System.out.println(inverted_index_query.get("1850").get(0).getPositionString());
        return inverted_index_query;
    }

    //a posting list for each term
    //questa funzione crea le posting list per ogni termine
    //quindi una posting per ogni doc id
    public List<Posting> createPosting(String docLine , String tfLine , String posLine){
        List<Posting> postings = new ArrayList<>();
        System.out.println(docLine); //--> doc_id
        System.out.println(tfLine);
        System.out.println(posLine);
        String[] docs_id = docLine.split(",");
        String[] tfs = tfLine.split(",");
        String[] posDoc = posLine.split(",");
        for (int i=0;i<docs_id.length;i++){
            List<Integer> posList = new ArrayList<>();
            if(posDoc[i].contains("-")){
                String[] tmp = posDoc[i].split("-");
                for(String pos : tmp){
                    posList.add(Integer.parseInt(pos));
                    //System.out.println("aggiunto pos = " + Integer.parseInt(pos));
                }
                Posting posting = new Posting(Integer.parseInt(docs_id[i]),Integer.parseInt(tfs[i]),posList);
                System.out.println("doc id : "+docs_id[i]+" pos : "+ posList + " tfs : " + tfs[i]);
                postings.add(posting);
            }else{
                posList.add(Integer.parseInt(posDoc[i]));
                Posting posting = new Posting(Integer.parseInt(docs_id[i]),Integer.parseInt(tfs[i]),posList);
                System.out.println("doc id  : "+docs_id[i]+" pos : "+ posList + " tfs : " + tfs[i]);
                postings.add(posting);
            }
        }
        return postings;
    }
}
