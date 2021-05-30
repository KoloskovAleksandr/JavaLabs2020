import ru.spbstu.pipeline.*;
import java.lang.reflect.*;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Manager {
    private final ManagerConfig _config;
    private final Logger _logger;
    private IPipelineStep _pipHead;
    private static final Class[] _constrParams = {Logger.class};

    private ArrayList<Thread> _pipelineThreads;

    public Manager(String configPath) {
        _logger = GetLogger();
        _config = new ManagerConfig();
        _pipelineThreads = new ArrayList<>();

        if(configPath != null){
            if(_config.SetFile(configPath) != RC.CODE_SUCCESS)
                _logger.warning("ERROR: Wrong manager config path");
            if(_config.ParseConfig() != RC.CODE_SUCCESS)
                _logger.warning("ERROR: Manager config parsing error");
        }
    }

    public Logger GetLogger() {
        Logger logger = Logger.getLogger("Manager");
        logger.setLevel(Level.ALL);
        try {
            FileHandler fh = new FileHandler("Logfile.txt");
            SimpleFormatter sf = new SimpleFormatter();
            fh.setFormatter(sf);
            logger.addHandler(fh);
        } catch (IOException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
        return logger;
    }

    public boolean FormPipeline() {
        if(_config.CheckConfig() != RC.CODE_SUCCESS) {
            _logger.warning("ERROR: Manager config grammar error");
            return false;
        }
        String[] chain = _config.GetParameter(ManagerGrammar.Tags.CHAIN.getCode())
                .split(ManagerGrammar.chainDelimiter());

        FileInputStream input;
        FileOutputStream output;
        try {
            input = new FileInputStream(_config.GetParameter(ManagerGrammar.Tags.INPUT.getCode()));
        } catch(IOException e) {
            _logger.warning("ERROR: Input stream error in Manager");
            return false;
        }
        try {
            output = new FileOutputStream(_config.GetParameter(ManagerGrammar.Tags.OUTPUT.getCode()));
        } catch(IOException e) {
            _logger.warning("ERROR: Output stream error in Manager");
            return false;
        }

        IPipelineStep prevElement = null;
        IPipelineStep curElement;
        Class castElement;
        for (String elem : chain) {
            try {
                castElement = Class.forName(_config.GetParameter(elem + ManagerGrammar.Tags.NAME.getCode()));
            } catch(ClassNotFoundException e) {
                _logger.warning("ERROR: Wrong class name for " + elem);
                return false;
            }
            try {
                curElement = (IPipelineStep) castElement.getConstructor(_constrParams).
                        newInstance(_logger);
            } catch(IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e){
                _logger.warning("ERROR: Instantiation error for " + elem);
                return false;
            }

            if (prevElement != null) {
                IConsumer currConsumer = (IConsumer) curElement;
                if (currConsumer.setProducer((IProducer) prevElement) != RC.CODE_SUCCESS) {
                    _logger.warning("ERROR: producer setting  error for " + elem);
                    return false;
                }

                currConsumer.getNotifier();
                if (prevElement.addNotifier(currConsumer.getNotifier()) != RC.CODE_SUCCESS) {
                    _logger.warning("ERROR: consumer setting  error for " + elem);
                    return false;
                }
            }

            if(curElement.setConfig(_config.GetParameter
                    (elem + ManagerGrammar.Tags.CONFIG_PATH.getCode())) != RC.CODE_SUCCESS){
                _logger.warning("ERROR: config setting  error for " + elem);
                return false;
            }

            if (elem.equals(ManagerGrammar.Tags.READER.getCode())) {
                _pipHead = curElement;
                if(((IReader) curElement).setInputStream(input) != RC.CODE_SUCCESS) {
                    _logger.warning("ERROR: input stream setting error for " + elem);
                    return false;
                }
            }
            if (elem.equals(ManagerGrammar.Tags.WRITER.getCode())) {
                if(((IWriter) curElement).setOutputStream(output) != RC.CODE_SUCCESS) {
                    _logger.warning("ERROR: output stream setting error for " + elem);
                    return false;
                }
            }
            prevElement = curElement;

            _pipelineThreads.add(new Thread(curElement));
        }
        return true;
    }

    public void ManageExecution() {
        for (Thread thread : _pipelineThreads) {
            thread.start();

            try {
                thread.join();
            } catch (InterruptedException e) {
                _logger.warning(RC.CODE_SYNCHRONIZATION_ERROR.toString());
            }
        }
    }
}

