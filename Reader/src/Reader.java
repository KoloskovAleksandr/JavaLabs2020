import ru.spbstu.pipeline.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.logging.Logger;

public class Reader implements IReader {
    private FileInputStream _reader;
    private int _bufferSize;
    private final TYPE[] _outputTypes = {TYPE.BYTE, TYPE.SHORT, TYPE.CHAR};
    private final Logger _logger;

    private HashMap<Integer, byte[]> _chunksToGive;
    private INotifier _consumerNotifier;

    @Override
    public TYPE[] getOutputTypes() {
        return _outputTypes;
    }

    @Override
    public IMediator getMediator(TYPE type) {
        switch(type) {
            case BYTE:
                return new MediatorByte();
            case SHORT:
                return new MediatorShort();
            case CHAR:
                return new MediatorChar();
            default:
                _logger.warning("Mediator of" + type.toString() + "is absent");
                return null;
        }
    }

    class MediatorByte implements IMediator {
        public byte[] getData(int chunkId) {
            synchronized (_chunksToGive) {
                return _chunksToGive.remove(chunkId);
            }
        }
    }

    class MediatorShort implements IMediator {
        public short[] getData(int chunkId) {
            byte[] data;
            synchronized (_chunksToGive) {
                data = _chunksToGive.remove(chunkId);
            }
            if (data == null) {
                return null;
            }

            short[] shorts = new short[data.length / 2];
            ByteBuffer.wrap(data).asShortBuffer().get(shorts);
            return shorts;
        }
    }

    class MediatorChar implements IMediator {
        public char[] getData(int chunkId) {
            byte[] data;
            synchronized (_chunksToGive) {
                data = _chunksToGive.remove(chunkId);
            }
            if (data == null) {
                return null;
            }

            return new String(_chunksToGive.remove(chunkId), StandardCharsets.UTF_16).toCharArray();
        }
    }

    public Reader(Logger logger){
        _logger = logger;
        _chunksToGive = new HashMap<>();
    }

    @Override
    public RC setInputStream(FileInputStream fileInputStream) {
        if(fileInputStream == null) {
            _logger.warning("ERROR: Invalid input stream in Reader");
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        _reader = fileInputStream;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConfig(String s) {
        if(s == null) {
            _logger.warning("ERROR: Invalid config path in Reader");
            return RC.CODE_INVALID_ARGUMENT;
        }
        else {
            File test = new File(s);
            if (!test.exists()) {
                _logger.warning("ERROR: Config file reading error in Reader");
                return RC.CODE_FAILED_TO_READ;
            }
        }

        ConfigParser config = new ConfigParser(new ReaderGrammar());
        RC error;
        if((error = config.SetFile(s)) != RC.CODE_SUCCESS) {
            _logger.warning("ERROR: Config reading error in Reader Config Parser");
            return error;
        }
        if((error = config.ParseConfig()) != RC.CODE_SUCCESS){
            _logger.warning("ERROR: Config parsing error in Reader Config Parser");
            return error;
        }
        if((error = config.CheckConfig()) != RC.CODE_SUCCESS){
            _logger.warning("ERROR: Invalid config error in Reader Config Parser");
            return error;
        }

        _bufferSize = Integer.parseInt(config .GetParameter(ReaderTags.BUFFER_SIZE.toString()));
        return RC.CODE_SUCCESS;
    }

    public RC execute() {
        if(_reader == null){
            _logger.warning("ERROR: Execution error in Reader, no input stream");
            return RC.CODE_INVALID_INPUT_STREAM;
        }

        RC error = RC.CODE_SUCCESS;

        int chunkId = 0;
        for(int isRead = 0; isRead != -1; chunkId++){
            byte[] buffer = new byte[_bufferSize];
            Arrays.fill(buffer, (byte)0);

            try {
                isRead = _reader.read(buffer, 0, _bufferSize);
                if(isRead == -1) continue;
            } catch (IOException e){
                _logger.warning("ERROR: Execution error in Reader, File reading error");
                return RC.CODE_FAILED_TO_READ;
            }

            synchronized (_chunksToGive) {
                _chunksToGive.put(chunkId, buffer);
                _consumerNotifier.notify(chunkId);
            }
        }

        synchronized (_chunksToGive) {
            _consumerNotifier.notify(chunkId);
        }

        try {
            _reader.close();
        } catch (IOException exception) {
            _logger.warning("ERROR: Execution error in Reader, input stream doesn't close");
            return RC.CODE_INVALID_INPUT_STREAM;
        }

        return error;
    }

    @Override
    public RC addNotifier(INotifier iNotifier) {
        _consumerNotifier = iNotifier;
        return RC.CODE_SUCCESS;
    }

    @Override
    public void run() {
        execute();
    }
}
