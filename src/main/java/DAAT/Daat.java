package DAAT;

import document_index.Document_index;
import inverted_index.Posting;
import lexicon.Lexicon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.Preprocess_doc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
//voglio copiare le posting list dei termini della query all'interno di una struttura dati
//quindi avro solo un inverted index dei termini della quey

//non ci sono metodi per saltare la lettura dell n righe in un file
public class Daat {

    public HashMap<Integer, Integer> docLens = new HashMap<>(); //lengths of the documents containing
    // at least once the terms of the query
    public Hashtable<Integer,Integer> ht_docindex = new Hashtable<>(); //for keeeping in memory the document index
    public Hashtable<String, Integer> ht_lexicon = new Hashtable<>();
    public Hashtable<String, Integer> doc_freqs = new Hashtable<>();


    public void daat(String query_string, int k) throws IOException {
        //HashMap<String, List<Posting>> inverted_index_query = new HashMap<>();
        Document_index document_index = new Document_index();
        ht_docindex = document_index.document_index_from_text("docs/document_index.txt");
        Lexicon lexicon = new Lexicon();
        ht_lexicon = lexicon.lexicon_from_text("docs/lexicon_tot.txt");
        doc_freqs = lexicon.lexicon_from_text_with_freqs("docs/lexicon_tot.txt");
        Preprocess_doc preprocessing = new Preprocess_doc();
        List<String> pro_query = new ArrayList<>();
        pro_query = preprocessing.preprocess_doc_optimized(query_string);
        //inverted_index_query = create_inverted_query(query_string);
        //System.out.println(pro_query);
        int query_len = pro_query.size();
        //System.out.println(query_len);
        HashMap<String, List<Posting>> inverted_lists = new HashMap<>();
        HashMap<String, Integer> query_freqs = new HashMap<>();
        for(String term: pro_query){
            //TODO 23/10/2022: aggiungi controllo duplicati perché sennò cicla più volte senza aggiungere nulla
            // di nuovo, però query freqs va comunque aggiornato
            List<Posting> postingList = new LinkedList<>();
            postingList = openList(term);
            inverted_lists.put(term, postingList);
            if(query_freqs.get(term) == null){
                query_freqs.put(term, 1);
            }
            else{
                int freq = query_freqs.get(term)+1;
                query_freqs.put(term, freq);
            }
        }
        //System.out.println(query_freqs);
        //System.out.println(inverted_lists);
        //System.out.println(docLens);
        int[][] rank = new int[10][2];
        HashMap<Integer, Double> scores = new HashMap<>();
        int docid = 0;
        int lastDoc = Collections.max(docLens.keySet());
        //do this until you processed every doc!!!
        while(docid<= lastDoc) {
            //System.out.println("HERE: " + docid);
            double score = 0.0;
            for (Map.Entry<String, List<Posting>> entry : inverted_lists.entrySet()) {
                String curTerm = entry.getKey();
                for (Posting p : entry.getValue()) {
                    if (p.getDocumentId() == docid && query_freqs.get(curTerm) != null && docLens.get(docid) != null && doc_freqs.get(curTerm)!= null) {
                        //apply scoring function
                        score += tfidf(query_freqs.get(curTerm), p.getTermFrequency(), docLens.get(p.getDocumentId()), query_len, doc_freqs.get(curTerm));
                    }
                }
            }
            //System.out.println(score);
            if(score!= 0.0) scores.put(docid, score);
            docid++;
            //docid = nextGEQ();
        }
        //sort the scores
        Comparator<Map.Entry<Integer, Double>> valueComparator =
                new Comparator<Map.Entry<Integer,Double>>() {
                    @Override
                    public int compare(Map.Entry<Integer, Double> e1, Map.Entry<Integer, Double> e2) {
                        Double v1 = e1.getValue();
                        Double v2 = e2.getValue();
                        return v2.compareTo(v1);
                    }
            };
        List<Map.Entry<Integer, Double>> listOfEntries
                = new ArrayList<Map.Entry<Integer, Double>>(scores.entrySet());
        Collections.sort(listOfEntries, valueComparator);
        LinkedHashMap<Integer, Double> sortedByValue = new LinkedHashMap<>(listOfEntries.size()); // copying entries from List to Map
        for(Map.Entry<Integer, Double> entry : listOfEntries){
            sortedByValue.put(entry.getKey(), entry.getValue());
        }
        //return the first k scores
        List<Map.Entry<Integer, Double>> sortedScores = sortedByValue.entrySet().stream()
                .limit(k)
                .collect(Collectors.toList());
        System.out.println(sortedScores);
        //TODO 22/10/2022: initialize the data structures and implement the scoring function
        // remember that documents have to be processed in parallel in increasing order of docid
        // also we need to implement the iterators to go through all the posting lists we need
        // so we should call the method for each term and not on the whole query!!! That's why
        // we need openList and closeList and also other iterators!
    }

    public List<Posting> openList(String query_string) throws IOException {
        //String inputLex = "docs/lexicon_tot.txt";
        String inputDocids = "docs/inverted_index_docids.txt";
        String inputFreqs = "docs/inverted_index_freq.txt";
        String inputPos = "docs/inverted_index_pos.txt";
        //created buffer to read file
        String lexLine = null;
        //in this structure we have all posting of term of query
        HashMap<String, List<Posting>> inverted_index_query = new HashMap<>();
        ArrayList<Posting> postings = new ArrayList<>();
        //iterate through all term of query
        //LineIterator itLex = FileUtils.lineIterator(new File(inputLex), "UTF-8");
        LineIterator itId = FileUtils.lineIterator(new File(inputDocids), "UTF-8");
        LineIterator itTf = FileUtils.lineIterator(new File(inputFreqs), "UTF-8");
        LineIterator itPos = FileUtils.lineIterator(new File(inputPos), "UTF-8");
        Set<String> globalTerms = new HashSet<>(ht_lexicon.keySet());
        TreeSet<String> sortedTerms = new TreeSet<>(globalTerms);
        Iterator<String> itTerms = sortedTerms.iterator();
        //System.out.println(query_string);
        List<Posting> postings_for_term = new ArrayList<>();
        int offset = 0;
        //when is founded the term , a copy in data structure of inverted index is made
        while(itTerms.hasNext()){
            String term = itTerms.next();
            if(term.equals(query_string)) {
                offset = ht_lexicon.get(term);
                break;
            }
            /*lexLine = itLex.nextLine();
            String[] inputs = lexLine.split(" ");
            if(inputs[0].equals(query_string)){
                offset = Integer.parseInt(inputs[1]); //--> this is the offset of term , we can use to retrive other info
                //System.out.println(offset);
                break;
            }*/

        }
        int i = 0;
        while(i<(offset-1)) {
            itId.nextLine();
            itTf.nextLine();
            itPos.nextLine();
            i++;
        }

        String docLine = null;
        String posLine = null;
        String tfLine = null;
        docLine = itId.nextLine();
        docLine = docLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
        posLine = itPos.nextLine();
        posLine = posLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
        tfLine = itTf.nextLine();
        tfLine = tfLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
        postings_for_term = createPosting(docLine,tfLine,posLine);
        return postings_for_term;

    }

    //TODO 22/10/2022: aggiungere la decompressione!!!!
    public HashMap<String, List<Posting>> create_inverted_query (String query_string) throws IOException {
        Preprocess_doc preprocessing = new Preprocess_doc();
        //preprocessing of query
        List<String> pro_query = new ArrayList<>();
        pro_query = preprocessing.preprocess_doc_optimized(query_string);
        System.out.println(pro_query);
        String inputLex = "docs/lexicon_tot.txt";
        String inputDocids = "docs/inverted_index_docids.txt";
        String inputFreqs = "docs/inverted_index_freq.txt";
        String inputPos = "docs/inverted_index_pos.txt";
        //created buffer to read file
        String lexLine = null;
        //in this structure we have all posting of term of query
        HashMap<String, List<Posting>> inverted_index_query = new HashMap<>();
        ArrayList<Posting> postings = new ArrayList<>();
        //iterate through all term of query
        for(String term : pro_query ){
            LineIterator itLex = FileUtils.lineIterator(new File(inputLex), "UTF-8");
            LineIterator itId = FileUtils.lineIterator(new File(inputDocids), "UTF-8");
            LineIterator itTf = FileUtils.lineIterator(new File(inputFreqs), "UTF-8");
            LineIterator itPos = FileUtils.lineIterator(new File(inputPos), "UTF-8");
            System.out.println(term);
            List<Posting> postings_for_term = new ArrayList<>();
            int offset = 0;
            //when is founded the term , a copy in data structure of inverted index is made
            while(itLex.hasNext()){
                lexLine = itLex.nextLine();
                String[] inputs = lexLine.split(" ");
                if(inputs[0].equals(term)){
                    offset = Integer.parseInt(inputs[1]); //--> this is the offset of term , we can use to retrive other info
                    //System.out.println(offset);
                }
            }
            int i = 0;
            while(i<(offset-1)) {
                itId.nextLine();
                itTf.nextLine();
                itPos.nextLine();
                i++;
            }

            String docLine = null;
            String posLine = null;
            String tfLine = null;
            docLine = itId.nextLine();
            docLine = docLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            posLine = itPos.nextLine();
            posLine = posLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            tfLine = itTf.nextLine();
            tfLine = tfLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            postings_for_term = createPosting(docLine,tfLine,posLine);
            inverted_index_query.put(term,postings_for_term);

        }

        //System.out.println(inverted_index_query.get("1850").get(0).getPositionString());
        return inverted_index_query;
    }


    public HashMap<String, List<Posting>> create_inverted_query_bin (String query_string) throws IOException {
        Preprocess_doc preprocessing = new Preprocess_doc();
        //preprocessing of query
        List<String> pro_query = new ArrayList<>();
        pro_query = preprocessing.preprocess_doc_optimized(query_string);
        //System.out.println(pro_query);
        String inputLex = "docs/lexicon_tot.bin";
        String inputDocids = "docs/inverted_index_docids.bin";
        String inputFreqs = "docs/inverted_index_freq.bin";
        String inputPos = "docs/inverted_index_pos.bin";
        //created buffer to read file
        /*BufferedReader itLex  = Files.newBufferedReader(Paths.get(inputLex), StandardCharsets.UTF_8);
        BufferedReader itId  = Files.newBufferedReader(Paths.get(inputDocids), StandardCharsets.UTF_8);
        BufferedReader itTf  = Files.newBufferedReader(Paths.get(inputFreqs), StandardCharsets.UTF_8);
        BufferedReader itPos  = Files.newBufferedReader(Paths.get(inputPos), StandardCharsets.UTF_8);*/
        String lexLine = null;
        //in this structure we have all posting of term of query
        HashMap<String, List<Posting>> inverted_index_query = new HashMap<>();
        ArrayList<Posting> postings = new ArrayList<>();
        //iterate through all term of query
        for(String term : pro_query ){
            LineIterator itLex = FileUtils.lineIterator(new File(inputLex), "UTF-8");
            LineIterator itId = FileUtils.lineIterator(new File(inputDocids), "UTF-8");
            LineIterator itTf = FileUtils.lineIterator(new File(inputFreqs), "UTF-8");
            LineIterator itPos = FileUtils.lineIterator(new File(inputPos), "UTF-8");
            //System.out.println(term);
            List<Posting> postings_for_term = new ArrayList<>();
            int offset = 0;
            //when is founded the term , a copy in data structure of inverted index is made
            while(itLex.hasNext()){
                lexLine = itLex.nextLine();
                String[] inputs = lexLine.split(" ");
                if(inputs[0].equals(term)){
                    offset = Integer.parseInt(inputs[1]); //--> this is the offset of term , we can use to retrive other info
                    //System.out.println(offset);
                }
            }
            int i = 0;
            while(i<(offset-1)) {
                itId.nextLine();
                itTf.nextLine();
                itPos.nextLine();
                i++;
            }
            /*while((lexLine = itLex.readLine()) != null){
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
            }*/
            String docLine = null;
            String posLine = null;
            String tfLine = null;
            //docLine = itId.readLine();
            docLine = itId.nextLine();
            docLine = docLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            //posLine = itPos.readLine();
            posLine = itPos.nextLine();
            posLine = posLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            //tfLine = itTf.readLine();
            tfLine = itTf.nextLine();
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
        //System.out.println(docLine); //--> doc_id
        //System.out.println(tfLine);
        //System.out.println(posLine);
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
                //System.out.println("doc id : "+docs_id[i]+" pos : "+ posList + " tfs : " + tfs[i]);
                postings.add(posting);
            }else{
                posList.add(Integer.parseInt(posDoc[i]));
                Posting posting = new Posting(Integer.parseInt(docs_id[i]),Integer.parseInt(tfs[i]),posList);
                //System.out.println("doc id : "+docs_id[i]+" pos : "+ posList + " tfs : " + tfs[i]);
                docLens.put(Integer.parseInt(docs_id[i]), ht_docindex.get(Integer.parseInt(docs_id[i])));
                postings.add(posting);
            }
        }
        return postings;
    }

    private Posting next(List<Posting> p, int i){
        return p.get(i);
    }

    private int getScore(Posting p){
        return p.getTermFrequency();
    }

    private double tfidf(int tf_q, int tf_d, int d_len, int q_len, int doc_freq){
        //System.out.println(tf_q + " " + tf_d + " "  + d_len + " " + q_len + " " + doc_freq);
        double factor1 = ((double)tf_q/q_len);
        double factor2 = (1.0 + Math.log(tf_d)*Math.log(ht_docindex.keySet().size()/doc_freq))/(double)d_len;
        return factor1*factor2;
    }
}
