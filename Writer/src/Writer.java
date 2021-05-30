import ru.spbstu.pipeline.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.logging.Logger;

public class Writer implements IWriter {
    private FileOutputStream _writer;
    private IMediator _prodMediator;
    private byte[] _buffer;
    private int _bufferSize;
    private int _bufferPos;
    private final TYPE[] _workingTypes = {TYPE.BYTE, TYPE.SHORT, TYPE.CHAR};
    private final Logger _logger;

    private LinkedList<Integer> _chunksToReceive;

    public Writer(Logger logger) {
        _logger = logger;
        _chunksToReceive = new LinkedList<>();
    }

    @Override
    public RC setOutputStream(FileOutputStream fileOutputStream) {
        if (fileOutputStream == null) {
            _logger.warning("ERROR: Invalid output stream in Writer");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }
        _writer = fileOutputStream;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConfig(String s) {
        if (s == null) {
            _logger.warning("ERROR: Invalid config path in Writer");
            return RC.CODE_INVALID_ARGUMENT;
        } else {
            File test = new File(s);
            if (!test.exists()) {
                _logger.warning("ERROR: Config file reading error in Writer");
                return RC.CODE_FAILED_TO_READ;
            }
        }

        ConfigParser config = new ConfigParser(new WriterGrammar());
        RC error = RC.CODE_SUCCESS;
        if ((error = config.SetFile(s)) != RC.CODE_SUCCESS) {
            _logger.warning("ERROR: Config reading error in Writer Config Parser");
            return error;
        }
        if ((error = config.ParseConfig()) != RC.CODE_SUCCESS) {
            _logger.warning("ERROR: Config parsing error in Writer Config Parser");
            return error;
        }
        if ((error = config.CheckConfig()) != RC.CODE_SUCCESS) {
            _logger.warning("ERROR: Invalid config error in Writer Config Parser");
            return error;
        }

        _bufferSize = Integer.parseInt(config.GetParameter(WriterTags.BUFFER_SIZE.toString()));
        _buffer = new byte[_bufferSize];
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IProducer producer) {
        if (producer == null) {
            _logger.warning("ERROR: Wrong producer in Writer");
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

    public RC execute(byte[] bytes) {
        if (bytes == null) {
            _logger.warning("ERROR: Invalid writing data");
            return RC.CODE_INVALID_ARGUMENT;
        }

        if (bytes.length < _bufferSize - _bufferPos) {
            System.arraycopy(bytes, 0, _buffer, _bufferPos, bytes.length);
            _bufferPos += bytes.length;
        }
        else {
            int bytesPos = 0;
            if (_bufferPos != 0) {
                bytesPos = _bufferSize - _bufferPos;
                byte[] tmp = Arrays.copyOf(_buffer, _bufferSize);
                System.arraycopy(bytes, 0, tmp, _bufferPos, bytesPos);
                _buffer = tmp;
                
                try {
                    _writer.write(_buffer, 0, _bufferSize);
                } catch (IOException e) {
                    _logger.warning("ERROR: Execution error in Writer");
                    return RC.CODE_FAILED_TO_WRITE;
                }
                _bufferPos = 0;
            }

            int fullArrays = (bytes.length - bytesPos) / _bufferSize;
            for (int i = 0; i < fullArrays; i++) {
                try {
                    _writer.write(bytes, bytesPos + _bufferSize * i, _bufferSize);
                } catch (IOException var7) {
                    _logger.warning("ERROR: Execution error in Writer");
                    return RC.CODE_FAILED_TO_WRITE;
                }
            }

            int residue = (bytes.length - bytesPos) % _bufferSize;
            if (residue != 0) {
                _bufferPos = residue;
                _buffer = Arrays.copyOfRange(bytes, _bufferSize * fullArrays, bytes.length - 1);
            } else {
                Arrays.fill(_buffer, (byte) 0);
            }
        }
        return RC.CODE_SUCCESS;
    }

    class WriterNotifier implements INotifier {
        @Override
        public RC notify(int chunkId) {
            synchronized (_chunksToReceive) {
                _chunksToReceive.add(chunkId);
            }
            synchronized (Writer.this) {
                Writer.this.notify();
            }
            return RC.CODE_SUCCESS;
        }
    }

    @Override
    public RC addNotifier(INotifier iNotifier) {
        return RC.CODE_SUCCESS;
    }

    @Override
    public INotifier getNotifier() {
        return new WriterNotifier();
    }

    @Override
    public void run() {
        while (true) {
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

            byte[] bytes = (byte[])_prodMediator.getData(chunkId);
            if (bytes != null) {
                RC result;
                if ((result = execute(bytes)) != RC.CODE_SUCCESS) {
                    _logger.warning(result.toString());
                    return;
                }

            } else {
                break;
            }
        }
    }
}