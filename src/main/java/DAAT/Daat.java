package DAAT;

import document_index.DocumentIndex;
import inverted_index.Posting;
import lexicon.Lexicon;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import preprocessing.PreprocessDoc;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
//voglio copiare le posting list dei termini della query all'interno di una struttura dati
//quindi avro solo un inverted index dei termini della quey

//non ci sono metodi per saltare la lettura dell n righe in un file
public class Daat {

    public HashMap<Integer, Integer> docLens = new HashMap<>(); //lengths of the documents containing
    // at least once the terms of the query
    public Hashtable<Integer,Integer> htDocindex = new Hashtable<>(); //for keeeping in memory the document index
    public Hashtable<String, Integer> htLexicon = new Hashtable<>();
    public Hashtable<String, Integer> docFreqs = new Hashtable<>();

    private final double k1 = 1.2;
    private final double b = 0.75;

    private int maxDocID;


    public Daat(){
        DocumentIndex document_index = new DocumentIndex();
        htDocindex = document_index.documentIndexFromText("docs/document_index.txt");
        Lexicon lexicon = new Lexicon();
        htLexicon = lexicon.lexiconFromText("docs/lexicon_tot.txt");
        docFreqs = lexicon.lexiconFromTextWithFreqs("docs/lexicon_tot.txt");
        maxDocID = htDocindex.size();
    }

    //TODO 30/10/2022: da controllare se va bene
    public void conjunctiveDaat(String query_string, int k) throws IOException {
        //HashMap<String, List<Posting>> inverted_index_query = new HashMap<>();
        /*Document_index document_index = new Document_index();
        ht_docindex = document_index.document_index_from_text("docs/document_index.txt");
        Lexicon lexicon = new Lexicon();
        ht_lexicon = lexicon.lexicon_from_text("docs/lexicon_tot.txt");
        doc_freqs = lexicon.lexicon_from_text_with_freqs("docs/lexicon_tot.txt");*/
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> pro_query = new ArrayList<>();
        pro_query = preprocessing.preprocess_doc_optimized(query_string);
        //inverted_index_query = create_inverted_query(query_string);
        //System.out.println(pro_query);
        int query_len = pro_query.size();
        //System.out.println(query_len);
        HashMap<String, LinkedList<Posting>> inverted_lists = new HashMap<>();
        HashMap<String, Integer> queryFreqs = new HashMap<>();
        for(String term: pro_query){
            //TODO 23/10/2022: aggiungi controllo duplicati perché sennò cicla più volte senza aggiungere nulla
            // di nuovo, però query freqs va comunque aggiornato
            LinkedList<Posting> postingList = new LinkedList<>();
            postingList = openList(term);
            inverted_lists.put(term, postingList);
            if(queryFreqs.get(term) == null){
                queryFreqs.put(term, 1);
            }
            else{
                int freq = queryFreqs.get(term)+1;
                queryFreqs.put(term, freq);
            }
        }
        //System.out.println(query_freqs);
        //System.out.println(inverted_lists);
        //System.out.println(docLens);
        HashMap<Integer, Double> scores = new HashMap<>();
        int docid = 0;
        //int lastDoc = Collections.max(docLens.keySet());
        //do this until you processed every doc!!!
        LinkedHashMap<Integer,Integer> docSet = new LinkedHashMap<>(docLens);
        //Iterator<String> itTerms = sortedTerms.iterator(); //--> iterator for all term in collection
        Iterator<Integer> itDocs = docSet.keySet().iterator();
        double avg_len = averageDocumentLength();
        double total = 0.0;
        while(docid < maxDocID) {
            //System.out.println("HERE: " + docid);
            double score = 0.0;
            int currDoc = 0;
            for (Map.Entry<String, LinkedList<Posting>> entry : inverted_lists.entrySet()) {
                if(docid == 0){
                    docid = nextGEQ(entry.getValue(), docid);
                }
                else{
                    currDoc = nextGEQ(entry.getValue(), docid);
                }
                if(currDoc > docid) break;
            }
            if(currDoc > docid){
                docid = currDoc; //docid not in the intersection
            }
            else{
                for (String curTerm: queryFreqs.keySet()) {
                    LinkedList<Posting> curList = inverted_lists.get(curTerm);
                    int freq  = getFreq(curList, docid);
                    if (queryFreqs.get(curTerm) != null && docLens.get(docid) != null && docFreqs.get(curTerm)!= null) {
                        //apply scoring function
                        //score += tfidf(p.getTermFrequency(), docLens.get(p.getDocumentId()), doc_freqs.get(curTerm));
                        //score += tfidfNorm(p.getTermFrequency(), docLens.get(p.getDocumentId()), doc_freqs.get(curTerm));
                        score += bm25Weight(freq, docLens.get(docid), docFreqs.get(curTerm), avg_len);
                    }
                }
                if(score!= 0.0) scores.put(docid, score);
                total += score;
                docid++;
            }
            //System.out.println(docid + " " + score);
            /*if(score!= 0.0) scores.put(docid, score);
            total += score;*/
            //docid = next(itDocs);
        }
        //System.out.println(scores);
        //normalize the scores
        normalizeScores(scores, total);
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
        /*List<Map.Entry<Integer, Double>> sortedScores = sortedByValue.entrySet().stream()
                .limit(k)
                .collect(Collectors.toList());*/
        List<Integer> sortedScores = sortedByValue.keySet().stream()
                .limit(k)
                .collect(Collectors.toList());
        docLens.clear();
        System.out.println("Top " + k + " documents for the conjunctive query \"" +  query_string + "\": " + sortedScores);

    }


    //TODO 30/11/2022: we need nextGEQ here too or is it fine as it is?
    public void disjunctiveDaat(String query_string, int k) throws IOException {
        //HashMap<String, List<Posting>> inverted_index_query = new HashMap<>();
        /*Document_index document_index = new Document_index();
        ht_docindex = document_index.document_index_from_text("docs/document_index.txt");
        Lexicon lexicon = new Lexicon();
        ht_lexicon = lexicon.lexicon_from_text("docs/lexicon_tot.txt");
        doc_freqs = lexicon.lexicon_from_text_with_freqs("docs/lexicon_tot.txt");*/
        PreprocessDoc preprocessing = new PreprocessDoc();
        List<String> pro_query = new ArrayList<>();
        pro_query = preprocessing.preprocess_doc_optimized(query_string);
        //inverted_index_query = create_inverted_query(query_string);
        //System.out.println(pro_query);
        int query_len = pro_query.size();
        //System.out.println(query_len);
        HashMap<String, LinkedList<Posting>> inverted_lists = new HashMap<>();
        HashMap<String, Integer> queryFreqs = new HashMap<>();
        for(String term: pro_query){
            //TODO 23/10/2022: aggiungi controllo duplicati perché sennò cicla più volte senza aggiungere nulla
            // di nuovo, però query freqs va comunque aggiornato
            LinkedList<Posting> postingList = new LinkedList<>();
            postingList = openList(term);
            inverted_lists.put(term, postingList);
            if(queryFreqs.get(term) == null){
                queryFreqs.put(term, 1);
            }
            else{
                int freq = queryFreqs.get(term)+1;
                queryFreqs.put(term, freq);
            }
        }
        //System.out.println(query_freqs);
        //System.out.println(inverted_lists);
        //System.out.println(docLens);
        HashMap<Integer, Double> scores = new HashMap<>();
        int docid = 0;
        //int lastDoc = Collections.max(docLens.keySet());
        //do this until you processed every doc!!!
        LinkedHashMap<Integer,Integer> docSet = new LinkedHashMap<>(docLens);
        //Iterator<String> itTerms = sortedTerms.iterator(); //--> iterator for all term in collection
        Iterator<Integer> itDocs = docSet.keySet().iterator();
        double avg_len = averageDocumentLength();
        double total = 0.0;
        while(docid < maxDocID) {
            //System.out.println("HERE: " + docid);
            double score = 0.0;
            for (Map.Entry<String, LinkedList<Posting>> entry : inverted_lists.entrySet()) {
                if(docid == 0){
                    docid = next(itDocs);
                    //docid = nextGEQ(entry.getValue(), docid);
                }
                String curTerm = entry.getKey();
                //System.out.println(curTerm + " " + docid);
                for (Posting p : entry.getValue()) {
                    //System.out.println(curTerm + " " + docid + " " + p.getDocumentId());
                    if (p.getDocumentId() == docid && queryFreqs.get(curTerm) != null && docLens.get(docid) != null && docFreqs.get(curTerm)!= null) {
                        //apply scoring function
                        //score += tfidf(p.getTermFrequency(), docLens.get(p.getDocumentId()), doc_freqs.get(curTerm));
                        //score += tfidfNorm(p.getTermFrequency(), docLens.get(p.getDocumentId()), doc_freqs.get(curTerm));
                        score += bm25Weight(p.getTermFrequency(), docLens.get(p.getDocumentId()), docFreqs.get(curTerm), avg_len);
                    }
                }
            }
            if(score!= 0.0) scores.put(docid, score);
            total += score;
            docid = next(itDocs);
        }
        //System.out.println(scores);
        //TODO 29/10/2022: decide whether or not to normalize bm25 scores
        //normalize the scores
        normalizeScores(scores, total);
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
        /*List<Map.Entry<Integer, Double>> sortedScores = sortedByValue.entrySet().stream()
                .limit(k)
                .collect(Collectors.toList());*/
        List<Integer> sortedScores = sortedByValue.keySet().stream()
                .limit(k)
                .collect(Collectors.toList());
        docLens.clear();
        System.out.println("Top " + k + " documents for the query \"" +  query_string + "\": " + sortedScores);

    }



    public LinkedList<Posting> openList(String query_string) throws IOException {
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
                offset = htLexicon.get(term) - 1;
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
        String docLine = (String) FileUtils.readLines(new File(inputDocids), "UTF-8").get(offset); //--> doc_id of selected term
        String tfLine = (String) FileUtils.readLines(new File(inputFreqs), "UTF-8").get(offset); //--> term freq of selected term
        String posLine = (String) FileUtils.readLines(new File(inputPos), "UTF-8").get(offset);
        //System.out.println(query_string + ": " + docLine + " " + tfLine);
        /*int i = 0;
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
        //docLine = docLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
        posLine = itPos.nextLine();
        //posLine = posLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
        tfLine = itTf.nextLine();
        //tfLine = tfLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");*/
        postings_for_term = createPosting(docLine,tfLine,posLine);
        return postings_for_term;

    }

    //TODO 30/10/2022: add closeList() method!

    //TODO 22/10/2022: aggiungere la decompressione!!!!
    public HashMap<String, List<Posting>> create_inverted_query (String query_string) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
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
            List<Posting> postings_for_term;
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
            //docLine = docLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            posLine = itPos.nextLine();
            //posLine = posLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            tfLine = itTf.nextLine();
            //tfLine = tfLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            postings_for_term = createPosting(docLine,tfLine,posLine);
            inverted_index_query.put(term,postings_for_term);

        }

        //System.out.println(inverted_index_query.get("1850").get(0).getPositionString());
        return inverted_index_query;
    }


    public HashMap<String, List<Posting>> create_inverted_query_bin (String query_string) throws IOException {
        PreprocessDoc preprocessing = new PreprocessDoc();
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
            List<Posting> postingsForTerm = new ArrayList<>();
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
            //docLine = docLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            //posLine = itPos.readLine();
            posLine = itPos.nextLine();
            //posLine = posLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            //tfLine = itTf.readLine();
            tfLine = itTf.nextLine();
            //tfLine = tfLine.replaceAll("\\s+", "").replaceAll("\\[", "").replaceAll("\\]","");
            postingsForTerm = createPosting(docLine,tfLine,posLine);
            inverted_index_query.put(term,postingsForTerm);

        }

        //System.out.println(inverted_index_query.get("1850").get(0).getPositionString());
        return inverted_index_query;
    }

    //a posting list for each term
    //questa funzione crea le posting list per ogni termine
    //quindi una posting per ogni doc id
    public LinkedList<Posting> createPosting(String docLine , String tfLine , String posLine){
        LinkedList<Posting> postings = new LinkedList<>();
        //System.out.println(docLine); //--> doc_id
        //System.out.println(tfLine);
        //System.out.println(posLine);
        String[] docs_id = docLine.split(" ");
        String[] tfs = tfLine.split(" ");
        String[] posDoc = posLine.split(" ");
        //String[] docs_id = docLine.split(",");
        //String[] tfs = tfLine.split(",");
        //String[] posDoc = posLine.split(",");
        for (int i=0;i<docs_id.length;i++){
            List<Integer> posList = new LinkedList<>();
            if(posDoc[i].contains("-")){
                String[] tmp = posDoc[i].split("-");
                for(String pos : tmp){
                    posList.add(Integer.parseInt(pos));
                    //System.out.println("aggiunto pos = " + Integer.parseInt(pos));
                }
                //TODO: add decompression here
                // non abbbiamo un intero ma una bit string, va decompressa!
                int doc = Integer.parseInt(docs_id[i]);
                int freq = Integer.parseInt(tfs[i]);
                Posting posting = new Posting(doc,freq,posList);
                docLens.put(doc, htDocindex.get(doc));
                //System.out.println("doc id : "+docs_id[i]+" pos : "+ posList + " tfs : " + tfs[i]);
                postings.add(posting);
            }else{
                posList.add(Integer.parseInt(posDoc[i]));
                Posting posting = new Posting(Integer.parseInt(docs_id[i]),Integer.parseInt(tfs[i]),posList);
                //System.out.println("doc id : "+docs_id[i]+" pos : "+ posList + " tfs : " + tfs[i]);
                docLens.put(Integer.parseInt(docs_id[i]), htDocindex.get(Integer.parseInt(docs_id[i])));
                postings.add(posting);
            }
        }
        return postings;
    }

    //iterate over the posting list ot get the desired term frequency, return 0 otherwise
    private int getFreq(LinkedList<Posting> postingList, int docid){
        for(Posting p: postingList){
            if(p.getDocumentId() == docid) return p.getTermFrequency();
        }
        return 0;
    }

    private Posting next(List<Posting> p, int i){
        return p.get(i);
    }
    private int next(Iterator<Integer> it){
        int next = maxDocID;
        if(it.hasNext()){
            next = it.next();
        }
        return next;
    }

    //nextGEQ(lp, k) find the next posting in list lp with docID >= k and
    //return its docID. Return value > MAXDID if none exists.
    private int nextGEQ(LinkedList<Posting> invertedLists, int prev) {
        int next = maxDocID;
        Iterator<Posting> it = invertedLists.iterator(); //iterate over the posting lists to finde the next docid
        while(it.hasNext()){
            next = it.next().getDocumentId();
            if(next >= prev){ //if the next docid is greater or equal the actual one, return it
                return next;
            }
        }
        return next;
    }

    //computes the average document length over the whole collection
    private double averageDocumentLength(){
        double avg = 0;
        for(int len: htDocindex.values()){
            avg+= len;
        }
        return avg/ htDocindex.keySet().size();
    }

    //tfidf scoring function for computing term frequency weights
    private double tfidf(int tf_d, int d_len, int doc_freq){
        //System.out.println(tf_q + " " + tf_d + " "  + d_len + " " + q_len + " " + doc_freq);
        return (1.0 + Math.log(tf_d)*Math.log(htDocindex.keySet().size()/doc_freq));
    }

    //normalized version of tfidf
    private double tfidfNorm(int tf_d, int d_len, int doc_freq){
        //System.out.println(tf_q + " " + tf_d + " "  + d_len + " " + q_len + " " + doc_freq);
        return (1.0 + Math.log(tf_d)*Math.log(htDocindex.keySet().size()/doc_freq))/(double)d_len;
    }

    //bm25 scoring function for computing weights for term frequency
    private double bm25Weight(int tf_d, int d_len, int doc_freq, double avg_len){
        return (((double)tf_d/((k1*((1-b) + b * (d_len/avg_len)))+tf_d)))*Math.log(htDocindex.keySet().size()/doc_freq);
    }

    //method for normalizing the scores obtained with bm25
    private void normalizeScores(HashMap<Integer, Double> sortedScores, double totalScore){
        for(Map.Entry<Integer,Double> e: sortedScores.entrySet()){
            sortedScores.put(e.getKey(), e.getValue()/totalScore);
        }
    }
}
