package org.example.transformers;

import org.example.dto.FileInfo;
import org.example.exceptions.TransformerException;
import org.example.tokenizer.ChunkTokenizer;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Optional;

import static java.lang.Math.max;
import static java.nio.file.StandardOpenOption.*;

public class ValidLineFilter extends AbstractFileWriter {
    private int validLinesAmount = 0;
    private long maxColumnsAmount = 0;

    public ValidLineFilter(Path inputFilePath, Path outputFilePath) {
        super(inputFilePath, outputFilePath);
    }

    private enum ColumnState {
        DELIMITER,
        VALUE_WRAPPER_FIRST,
        VALUE,
        VALUE_WRAPPER_SECOND,
        NEW_LINE,
        SKIP_UNTIL_NEW_LINE
    }

    @Override
    public Path transform(Path input) throws TransformerException {
        return filter();
    }

    private Optional<ColumnState> stateByFirstLexeme(ChunkTokenizer chunk) {
        chunk.skipWhitespaceIfPresent();
        var curCode = chunk.get();

        if (curCode == -1) {
            return Optional.empty();
        }

        var curSymb = (char) curCode;
        ColumnState stateResult = null;

        if (chunk.isDigit(curSymb)) {
            stateResult = ColumnState.VALUE;
        } else if (chunk.isNewLine(curSymb)) {
            stateResult = ColumnState.NEW_LINE;
        } else if (chunk.isColumnDelimiter(curSymb)) {
            stateResult = ColumnState.DELIMITER;
        } else if (chunk.isValueWrapper(curSymb)) {
            stateResult = ColumnState.VALUE_WRAPPER_SECOND;
        }

        if (stateResult == null) {
            return Optional.empty();
        }

        return Optional.of(stateResult);
    }

    public Path filter() throws TransformerException {
        try (RandomAccessFile raf = new RandomAccessFile(inputFilePath.toString(), "r");
             FileChannel fileWriterChannel = FileChannel.open(outputFilePath, TRUNCATE_EXISTING, WRITE, CREATE)) {

            long curLineStart = raf.getFilePointer();
            var buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            var isLineCorrect = true;
            var state = ColumnState.DELIMITER;
            var prevState = state;

            while ((bytesRead = raf.read(buffer)) != -1) {
                var chunk = new ChunkTokenizer(new String(buffer, 0, bytesRead));

                if (state != ColumnState.SKIP_UNTIL_NEW_LINE && chunk.hasRemainingCharacters()) {
                    var optionalState = stateByFirstLexeme(chunk);
                    if (optionalState.isPresent()) {
                        if (state == optionalState.get() &&
                                state != ColumnState.VALUE_WRAPPER_FIRST && state != ColumnState.VALUE_WRAPPER_SECOND) {
                            state = prevState;
                        }
                    }
                }

                while (chunk.hasRemainingCharacters()) {
                    prevState = state;

                    switch (state) {
                        case VALUE -> {
                            isLineCorrect = chunk.skipValueWrapper();
                            state = ColumnState.VALUE_WRAPPER_SECOND;
                        }
                        case DELIMITER -> {
                            isLineCorrect = chunk.skipValueWrapper();
                            state = ColumnState.VALUE_WRAPPER_FIRST;
                        }
                        case VALUE_WRAPPER_FIRST -> {
                            isLineCorrect = chunk.skipDigits();
                            state = ColumnState.VALUE;
                        }
                        case VALUE_WRAPPER_SECOND -> {
                            isLineCorrect = chunk.skipColumnDelimiter();
                            state = ColumnState.DELIMITER;
                            if (!isLineCorrect) {
                                isLineCorrect = chunk.skipNewLines();
                                state = ColumnState.NEW_LINE;
                            }
                        }
                        case SKIP_UNTIL_NEW_LINE -> {
                            int newLineInd = chunk.skipUtilNewValidLine();
                            if (newLineInd != -1) {
                                isLineCorrect = true;
                                state = ColumnState.DELIMITER;
                                curLineStart = seekIndexByBufferIndex(raf.getFilePointer(), raf.length(), newLineInd, bytesRead, chunk);
                            }
                        }

                        default -> throw new RuntimeException("Unexpected state");
                    }

                    if (isLineCorrect && (state == ColumnState.NEW_LINE ||
                            state == ColumnState.VALUE_WRAPPER_SECOND && isEOF(bytesRead, buffer.length, chunk))) {
                        raf.seek(curLineStart);
                        curLineStart = addValidLineToOutputFile(raf, fileWriterChannel);
                        if (!isEOF(bytesRead, buffer.length, chunk)) {
                            raf.seek(curLineStart);
                        }
                        state = ColumnState.SKIP_UNTIL_NEW_LINE;
                        break;
                    } else if (!isLineCorrect) {
                        state = ColumnState.SKIP_UNTIL_NEW_LINE;
                    }
                }
            }
        } catch (FileNotFoundException e) {
            throw new TransformerException("Failed to find file when filtering.", inputFilePath, outputFilePath, e);
        } catch (IOException e) {
            throw new TransformerException("Failed to filter input file " + inputFilePath, e);
        }

        return outputFilePath;
    }

    private boolean isEOF(int bytesRead, int bufferLen, ChunkTokenizer chunk) {
        return bytesRead < bufferLen && !chunk.hasRemainingCharacters();
    }

    private long addValidLineToOutputFile(RandomAccessFile rafReader, FileChannel fileWriterChannel) throws IOException {
        var buffer = new byte[BUFFER_SIZE];
        ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        int bytesRead;
        var lineIsEnded = false;
        long delimitersAmount = 0;
        long filePointerToBufferWithNewLine = rafReader.getFilePointer();

        while (!lineIsEnded && (bytesRead = rafReader.read(buffer)) != -1) {
            var chunk = new ChunkTokenizer(new String(buffer, 0, bytesRead));
            var newLineIndex = chunk.indexOfNewLine();

            if (newLineIndex == -1) {
                byteBuffer.put(buffer, 0, bytesRead);
                delimitersAmount += chunk.countColumnDelimiters();
                filePointerToBufferWithNewLine = rafReader.getFilePointer();
            } else {
                lineIsEnded = true;
                byteBuffer.put(buffer, 0, newLineIndex + 1);
                delimitersAmount += chunk.countColumnDelimitersWithRightBorder(newLineIndex);
            }

            byteBuffer.flip();
            while (byteBuffer.hasRemaining()) {
                fileWriterChannel.write(byteBuffer);
            }
            byteBuffer.clear();
        }

        validLinesAmount++;
        maxColumnsAmount = max(maxColumnsAmount, delimitersAmount + 1);

        return filePointerToBufferWithNewLine;
    }

    public FileInfo fileInfo() {
        return new FileInfo(validLinesAmount, maxColumnsAmount);
    }
}
