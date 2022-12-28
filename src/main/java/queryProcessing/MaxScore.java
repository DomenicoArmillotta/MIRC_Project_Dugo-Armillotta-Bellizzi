package queryProcessing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MaxScore {
    /*

    First, we need the inverted index

    For each query term, you will need to retrieve the posting list for that term from the inverted index.
    In our case we have a query for each term in the lexicon; each query is the single term

    For each document in the posting list, you will need to compute the score of the document for the query.
    This can be done using a scoring function such as BM25 or tf-idf. --> we use bm25

    For each query, you will need to compute the maximum score among all the documents in the posting list for that query.
    This will be the maxscore for the query.

    Once you have computed the maxscores for all the queries, you can store them in a data structure such as an array or a
    dictionary for later use.
     */
    private Map<String, List<Integer>> invertedIndex;
    private Map<String, Double> maxScores;
    public void computeMaxScores() {
        // Create a map to store the maxscores for each query
        maxScores = new HashMap<>();

        // Iterate through all the queries
        //Take the terms from the lexicon and iterate through them
        List<String> queries = new ArrayList<>(); //è solo per definire lo scheletro del codice
        for (String query : queries) {
            // Retrieve the posting list for the query from the inverted index
            List<Integer> postingList = invertedIndex.get(query);

            // Initialize the maxscore for the query to 0
            double maxScore = 0;

            // Iterate through the documents in the posting list
            for (int docId : postingList) {
                // Compute the score of the document for the query using a scoring function such as BM25 or tf-idf
                double score = 0.0; //TODO: use bm25!!!

                // Update the maxscore if necessary
                maxScore = Math.max(maxScore, score);
            }

            // Store the maxscore for the query in the maxScores map
            maxScores.put(query, maxScore);
        }
    }
}
