# MIRC_Project:


## Da fare:
- sistemare le query disjunctive per vedere come risparmiare tempo (URGENTE!!!!)
- riguardare e pulire il codice
- aggiugnere una classe per la cache: crea un file con i termini del lexicon più cercati e li gestisce con politica LRU (la creazione e ricerca su file è analoga a quella del lexicon)
- vedere se vogliamo avere l'alternativa di usare tfidf e quindi aggiungere nel lexicon il term upper bound calcolato con tfidf
- togliere la cf appena siamo scuri al 101% che non la usiamo (aggiornare lunghezza entry del lexicon, vale anche per l'aggiunta della tub di tfidf)
- Testare con tutta la collezione (nell'ordine: creare docindex e parametri, eseguire spimi, calcolo maxscores e skipblocks)
- Fare la documentazione e commentare per bene tutto il codice
- correggere alcune funzioni (esempio i parametri di nextgeq e il ritorno di disjunctive daat)