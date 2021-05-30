import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import ru.spbstu.pipeline.*;

public class ConfigParser {
    private BufferedReader _fileReader;
    private final BaseGrammar _grammar;
    private final HashMap<String, String> _params = new HashMap<>();

    ConfigParser(BaseGrammar grammar){
        _grammar = grammar;
    }

    public void AddParameter(String tag, String param) {
        _params.put(tag, param);
    }

    public String GetParameter(String tag) {
        return _params.get(tag);
    }

    public RC SetFile(String ConfigFile) {
        try {
            _fileReader = new BufferedReader(new FileReader(ConfigFile));
        } catch (IOException e) {
            return RC.CODE_INVALID_ARGUMENT;
        }
        return RC.CODE_SUCCESS;
    }

    public RC ParseConfig() {
        try {
            String[] splitLine;
            for (int i = 0, num = _grammar.numberTokens(); i < num; i++) {
                splitLine = _fileReader.readLine().split(_grammar.delimiter());
                if (splitLine.length == 2) {
                    AddParameter(splitLine[0], splitLine[1]);
                }
            }
        } catch (IOException e) {
            return RC.CODE_CONFIG_GRAMMAR_ERROR;
        }
        return RC.CODE_SUCCESS;
    }

    public RC CheckConfig() {
        for (int i = 0, num = _grammar.numberTokens(); i < num; i++)
            if (!_params.containsKey(_grammar.token(i)))
                return RC.CODE_CONFIG_GRAMMAR_ERROR;
        return RC.CODE_SUCCESS;
    }
}
