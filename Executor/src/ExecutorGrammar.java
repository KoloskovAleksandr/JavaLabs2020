import ru.spbstu.pipeline.BaseGrammar;

import java.util.Arrays;

public class ExecutorGrammar extends BaseGrammar {
    public ExecutorGrammar() {
        super(Grammar());
    }
    public static String[] Grammar(){
        ExecutorTags[] tags = ExecutorTags.values();
        String[] grammar = new String[tags.length];
        for(int i = 0; i < tags.length; i++)
            grammar[i] = tags[i].toString();
        return grammar;
    }
}