import ru.spbstu.pipeline.BaseGrammar;

import java.util.Arrays;

public class ReaderGrammar extends BaseGrammar {
    public ReaderGrammar() {
        super(Grammar());
    }
    public static String[] Grammar(){
        ReaderTags[] tags = ReaderTags.values();
        String[] grammar = new String[tags.length];
        for(int i = 0; i < tags.length; i++)
            grammar[i] = tags[i].toString();
        return grammar;
    }}
