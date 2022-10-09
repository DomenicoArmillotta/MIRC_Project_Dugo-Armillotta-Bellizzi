package preprocessing;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Tokenizer {

    /**
     * Function that takes in input a document and returns the tokens of the document
     * @param  s      the string that represent a document
     * @return        tokens of that document
     */
    public List<String> tokenize(String s){
        String[] input = s.split("\t");
        StringTokenizer st = new StringTokenizer(input[0]);
        List<String> terms = new ArrayList<>();
        while (st.hasMoreTokens()) {
            terms.add(st.nextToken());
        }
        return  terms;
    }

}
