package indexing;

public class SkipBlocksGenerator {
    //TODO: in questa classe dobbiamo generare un file contenente gli skip blocks;
    // utilizziamo docFreq per leggere numero di posting di ogni lista, si fa la radice quadra, e si segna il docid finale per smettere
    // di leggere un blocco; in lettura per tf bisogna procedere parallelamente utilizzando un metodo getFreq nel mentre che si legge
    // la lista compressa di docid (da fare nella classe Daat). In sintesi qui bisogna fare:
    // - lettura lexicon parola per parola;
    // - lettura invIndex per leggere le liste a blocchi e segnare per ogni blocco la lunghezza in bits
    // - scrittura di coppie (endocid, bitslen) nel file
    // - serve tenere un puntatore nel lexicon --> questo passaggio conviene farlo alla fine del merging?
    // NOTA BENE: forse abbiamo già tutte le info che ci servono per lo skipping, l'unico problema è che ogni volta
    // bisogna ricalcolare la radice per sapere quanti skip blocks abbiamo, e potrebbe essere lento
}
