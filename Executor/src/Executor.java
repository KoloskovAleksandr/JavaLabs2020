import ru.spbstu.pipeline.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Logger;

public class Executor implements IExecutor {
    private IMediator _prodMediator;
    private static final int LONG_SIZE = 8;
    private int _shiftQuantity;
    private int _singleShift;
    private byte[] _product;
    private final TYPE[] _outputTypes = {TYPE.BYTE, TYPE.SHORT, TYPE.CHAR};
    private final TYPE[] _workingTypes = {TYPE.BYTE};
    private final Logger _logger;

    private HashMap<Integer, byte[]> _chunksToGive;
    private LinkedList<Integer> _chunksToReceive;
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

            return new String(data, StandardCharsets.UTF_16).toCharArray();
        }
    }

    public Executor(Logger logger){
        _logger = logger;
        _chunksToReceive = new LinkedList<>();
        _chunksToGive = new HashMap<>();
    }

    @Override
    public RC setConfig(String s) {
        if(s == null) {
            _logger.warning("ERROR: Invalid config path in Executor");
            return RC.CODE_INVALID_ARGUMENT;
        }
        else {
            File test = new File(s);
            if (!test.exists()) {
                _logger.warning("ERROR: Config file reading error in Executor");
                return RC.CODE_FAILED_TO_READ;
            }
        }

        ConfigParser config = new ConfigParser(new ExecutorGrammar());
        RC error;
        if((error = config.SetFile(s)) != RC.CODE_SUCCESS) {
            _logger.warning("ERROR: Config reading error in Executor Config Parser");
            return error;
        }
        if((error = config.ParseConfig()) != RC.CODE_SUCCESS){
            _logger.warning("ERROR: Config parsing error in Executor Config Parser");
            return error;
        }
        if((error = config.CheckConfig()) != RC.CODE_SUCCESS){
            _logger.warning("ERROR: Invalid config error in Executor Config Parser");
            return error;
        }

        _singleShift = Integer.parseInt(config.GetParameter(ExecutorTags.SINGLE_SHIFT.toString()));
        _shiftQuantity = Integer.parseInt(config.GetParameter(ExecutorTags.SHIFT_QUANTITY.toString()));
        if(_singleShift < LONG_SIZE || _singleShift % LONG_SIZE != 0){
            _logger.warning("ERROR: Invalid params(SingleShift) in Executor config");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IProducer producer) {
        if (producer == null) {
            _logger.warning("ERROR: Wrong producer in Executor");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
        }
            TYPE[] producerTypes = producer.getOutputTypes();
            for (TYPE workingType : _workingTypes) {
                for (TYPE producerType : producerTypes) {
                    if (workingType == producerType) {
                        _prodMediator = producer.getMediator(workingType);
                        return RC.CODE_SUCCESS;
                    }
                }
            }

            _logger.warning("No compatibility with producer");
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
    }

    public RC execute(byte[] bytes, int chunkId) {
        if (bytes == null) {
            return RC.CODE_SUCCESS;
        }
        int bufferSize = bytes.length % LONG_SIZE == 0 ?
                bytes.length : bytes.length + LONG_SIZE - bytes.length % LONG_SIZE;
        byte[] buffer = new byte[bufferSize];
        System.arraycopy(bytes, 0, buffer, 0, bytes.length);

        _product = new byte[bufferSize];
        int refinedShift = _singleShift * _shiftQuantity;
        refinedShift = (bufferSize > refinedShift) ? refinedShift : bufferSize % refinedShift;
        System.arraycopy(buffer, 0, _product, refinedShift, bufferSize - refinedShift);
        System.arraycopy(buffer, bufferSize - refinedShift, _product, 0, refinedShift);

        synchronized (_chunksToGive) {
            _chunksToGive.put(chunkId, _product);
        }
        return RC.CODE_SUCCESS;
    }

    class ExecutorNotifier implements INotifier {
        @Override
        public RC notify(int chunkId) {
            synchronized (_chunksToReceive) {
                _chunksToReceive.add(chunkId);
            }
            synchronized (Executor.this) {
                Executor.this.notify();
            }
            return RC.CODE_SUCCESS;
        }
    }

    @Override
    public RC addNotifier(INotifier iNotifier) {
        _consumerNotifier = iNotifier;
        return RC.CODE_SUCCESS;
    }

    @Override
    public INotifier getNotifier() {
        return new ExecutorNotifier();
    }

    @Override
    public void run() {
        byte[] bytes = null;

        do {
            Integer chunkId;
            synchronized (_chunksToReceive) {
                chunkId = _chunksToReceive.poll();
            }

            if (chunkId == null) {
                try {
                    synchronized (this) {
                        wait();
                    }
                    continue;

                } catch (InterruptedException e) {
                    _logger.warning(RC.CODE_SYNCHRONIZATION_ERROR.toString());
                    return;
                }
            }

            bytes = (byte[])_prodMediator.getData(chunkId);
            RC result;
            if ((result = execute(bytes, chunkId)) != RC.CODE_SUCCESS) {
                _logger.warning(result.toString());
                return;
            }
            _consumerNotifier.notify(chunkId);

        } while (bytes != null);
    }
}
