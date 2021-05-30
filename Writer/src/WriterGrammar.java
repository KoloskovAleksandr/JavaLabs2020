import ru.spbstu.pipeline.BaseGrammar;

import java.util.Arrays;

public class WriterGrammar extends BaseGrammar {
    public WriterGrammar() {
        super(Grammar());
    }
    public static String[] Grammar(){
        WriterTags[] tags = WriterTags.values();
        String[] grammar = new String[tags.length];
        for(int i = 0; i < tags.length; i++)
            grammar[i] = tags[i].toString();
        return grammar;
    }
}
