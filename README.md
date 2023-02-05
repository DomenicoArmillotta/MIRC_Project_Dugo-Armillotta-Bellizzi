# Multimedia Information Retrieval and Computer Vision Project A.A 2022/23

This project consists in two different programs, one to build an index data structure, made of an inverted index, a lexicon with all the distinct terms in the collection used to build the index, and a documetn Index, and the other to make queries to obtain a ranking of documents in decreasing order of probability of relevance, computed using scoring functions on the documents in which the term of the queries appear. The two main classes to execute are:

- Indexer
- QueryProcessor

## Indexer
This program requires two parameters: a zip file containing the collection of documents on which the index will be built, and the other which is a boolean to specify if including stopwords and unstemmed terms in the vocabulary (false) or not (true).
The program will take some time to run completely, and at the end it will produce, besides the intermediate files, the files required for the second program to work:
- lexicon: contains the distinct terms of the collection with some statistics and the pointers to the posting lists
- docids: contains the docIDs of the posting lists
- tfs: contains the term frequencies of the posting lists
- docIndex: contains the document table, with a docno, a docID and the document length
- skipInfo: contains some info used during query processing to implement the skipping functionality
- parameters: contains some parameters computed during indexing, for instance the average document length


## QueryProcessor

This program can be executed after executing the first one. When running it, the user can insert a query that will be processed or insert "END" to end the program.
Then, other parameters are required:
- mode: 0 if the user wants to make a conjunctive query (AND), 1 if disjunctive (OR)
- scoring function: 0 to use TFIDF as scoring function, 1 to use BM25
- rank: it has to be greater than 0, it's the number of top documents the user wants to retrieve

After inserting all the parameters, the program will return the top k ranking of documents with respect to the query. Then it allows the user to make another query until the user itself digits "END".
