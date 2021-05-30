import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import ru.spbstu.pipeline.*;

public class ManagerConfig {
    private BufferedReader _fileReader;
    private final HashMap<String, String> _params = new HashMap<>();

    public void AddParameter(String tag, String param) { _params.put(tag, param); }
    public boolean CheckParameter(String tag) { return _params.containsKey(tag); }
    public String GetParameter(String tag) {
        return _params.get(tag);
    }

    public RC SetFile(String ConfigFile) {
        if(ConfigFile == null)
            return RC.CODE_INVALID_ARGUMENT;
        try {
            _fileReader = new BufferedReader(new FileReader(ConfigFile));
        } catch (IOException e) {
            return RC.CODE_INVALID_ARGUMENT;
        }
        return RC.CODE_SUCCESS;
    }

    public RC ParseConfig() {
        try {
            String readLine;
            String[] splitLine;
            while((readLine = _fileReader.readLine()) != null){
                splitLine = readLine.split(ManagerGrammar.delimiter());
                if (splitLine.length == 2) {
                    AddParameter(splitLine[0], splitLine[1]);
                }
                else { return RC.CODE_CONFIG_GRAMMAR_ERROR; }
            }
        } catch (IOException e) {
            return RC.CODE_FAILED_TO_READ;
        }
        return RC.CODE_SUCCESS;
    }

    public RC CheckConfig() {
        if(!CheckParameter(ManagerGrammar.Tags.INPUT.getCode()) ||
                !CheckParameter(ManagerGrammar.Tags.OUTPUT.getCode()) ||
                !CheckParameter(ManagerGrammar.Tags.CHAIN.getCode()))
            return RC.CODE_CONFIG_GRAMMAR_ERROR;

        String[] chain = GetParameter(ManagerGrammar.Tags.CHAIN.getCode()).
                split(ManagerGrammar.chainDelimiter());
        if(chain.length < 2 ||
                !chain[0].equals(ManagerGrammar.Tags.READER.getCode()) ||
                !chain[chain.length - 1].equals(ManagerGrammar.Tags.WRITER.getCode()))
            return RC.CODE_CONFIG_GRAMMAR_ERROR;

        for (String s : chain)
            if (!CheckParameter(s + ManagerGrammar.Tags.NAME.getCode()) ||
                    !CheckParameter(s + ManagerGrammar.Tags.CONFIG_PATH.getCode()))
                return RC.CODE_CONFIG_GRAMMAR_ERROR;

        return RC.CODE_SUCCESS;
    }
}