public class ManagerGrammar {
    private static final String separator = "=";
    private static final String chainSep = "->";

    public enum Tags{
        INPUT("INPUT"),
        OUTPUT("OUTPUT"),
        READER("READER"),
        WRITER("WRITER"),
        NAME("_NAME"),
        CONFIG_PATH("_CONFIG"),
        CHAIN("CHAIN");

        private final String code;
        Tags(String code){
            this.code = code;
        }
        public String getCode(){ return code;}
    }

    public static String delimiter() { return separator; }

    public static String chainDelimiter(){ return chainSep; }
}
