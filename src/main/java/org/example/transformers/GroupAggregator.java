package org.example.transformers;

import org.example.dto.FileInfo;
import org.example.exceptions.ExternalSortException;
import org.example.exceptions.TokenizerException;
import org.example.exceptions.TransformerException;
import org.example.external.sort.ExternalFileSorter;
import org.example.tokenizer.ChunkTokenizer;
import org.example.wrappers.OutputChannel;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.IntStream;

import static java.nio.file.StandardOpenOption.*;

public class GroupAggregator extends AbstractFileWriter {
    private static final String EMPTY_VALUE = "\"\"";
    private static final String TMP_DELIMITER = "|"; // expected to be 1 symbol
    private final Path duplicateRemovalInputPath;
    private final FileInfo fileInfo;
    private final int[] parent;
    private final int[] size;

    /**
     * Contract: input file that contains matrix must be valid and transposed.
     *
     * @param inputFilePath  path to file that contains input transposed matrix
     * @param outputFilePath path to file that will contain output
     * @param fileInfo       information about file - line amount (now it is column amount as matrix is transposed).
     */
    public GroupAggregator(Path inputFilePath, Path outputFilePath, FileInfo fileInfo, Path duplicateRemovalInputPath) {
        super(inputFilePath, outputFilePath);
        this.fileInfo = fileInfo;
        parent = new int[fileInfo.validLinesAmount()];
        IntStream.range(0, parent.length).forEach(i -> parent[i] = i);
        size = new int[fileInfo.validLinesAmount()];
        Arrays.fill(size, 1);
        this.duplicateRemovalInputPath = duplicateRemovalInputPath;
    }

    @Override
    public Path transform(Path input) throws TransformerException {
        return aggregateGroups();
    }

    public Path aggregateGroups() throws TransformerException {
        computeDSU();
        makeParentsToBeRoots();
        var rootLinePath = createTemporaryFileRootLine();
        final var sortResultPath = Paths.get("").toAbsolutePath().resolve("root_line_sorted.txt");

        try {
            ExternalFileSorter.sortByGroupSize(rootLinePath, sortResultPath, TMP_DELIMITER, size);
        } catch (ExternalSortException e) {
            throw new TransformerException("Failed to sort file of format: <group>" + TMP_DELIMITER + "<line>.", e);
        }

        var groupsAmount = countGroupAmountWithSizeMoreThanOne();
        var output = writeResultToOutputFile(sortResultPath, groupsAmount);

        try {
            Files.delete(rootLinePath);
            Files.delete(sortResultPath);
        } catch (IOException e) {
            throw new TransformerException("Failed deleting temporary files after group aggregation: " +
                    rootLinePath + "; " + sortResultPath, e);
        }

        return output;
    }

    private Path writeResultToOutputFile(Path sortedRootLine, int groupsAmount) throws TransformerException {
        var resultPath = Paths.get("").toAbsolutePath().resolve("result.txt");

        try (var inputChannel = FileChannel.open(sortedRootLine, READ);
             var outputNIOChannel = FileChannel.open(resultPath, TRUNCATE_EXISTING, WRITE, CREATE)) {

            var outputChannel = new OutputChannel(outputNIOChannel);
            outputChannel.write(groupsAmount + "\n");

            int bytesRead;
            var buffer = ByteBuffer.allocate(BUFFER_SIZE);
            int prevGroup = -1;
            int groupToPrint = 1;
            String groupPart = "";

            while ((bytesRead = inputChannel.read(buffer)) != -1) {
                var chunk = new ChunkTokenizer(new String(buffer.array(), 0, bytesRead));

                while (chunk.hasRemainingCharacters()) {
                    var delimiterIndex = chunk.indexOfFromCurrentIndex(c -> c == TMP_DELIMITER.charAt(0));
                    var newLineIndex = chunk.indexOfNewLineFromCurrentIndex();
                    var bothAreFound = (delimiterIndex != -1) && (newLineIndex != -1);

                    if ((delimiterIndex != -1 && newLineIndex == -1) || (bothAreFound && delimiterIndex < newLineIndex)) {
                        int group;
                        if (!groupPart.isEmpty()) {
                            group = Integer.parseInt(groupPart + chunk.substring(delimiterIndex));
                            groupPart = "";
                        } else {
                            group = Integer.parseInt(chunk.substring(delimiterIndex));
                        }
                        chunk.setIndex(delimiterIndex + 1);

                        if (group != prevGroup) {
                            outputChannel.write("\nГруппа " + (groupToPrint) + "\n");
                            prevGroup = group;
                            groupToPrint++;
                        }
                    } else if ((newLineIndex != -1 && delimiterIndex == -1) || (bothAreFound && newLineIndex < delimiterIndex)) {
                        outputChannel.write(chunk.substring(newLineIndex + 1));
                        chunk.setIndex(newLineIndex + 1);
                        if (chunk.indexOfFromCurrentIndex(c -> c == TMP_DELIMITER.charAt(0)) == -1) {
                            groupPart = chunk.substring(chunk.size());
                            chunk.setIndex(chunk.size());
                        }
                    } else {
                        outputChannel.write(chunk.substring(chunk.size()));
                        chunk.setIndex(chunk.size());
                    }
                }
                buffer.clear();
            }
        } catch (IOException e) {
            throw new TransformerException("Final stage of group aggregation failed.", sortedRootLine, resultPath, e);
        } catch (TokenizerException e) {
            throw new TransformerException("Failed to parse file: " + sortedRootLine, e);
        }

        return resultPath;
    }

    private int countGroupAmountWithSizeMoreThanOne() {
        var counted = new HashSet<Integer>();
        int res = 0;

        for (int i = 0; i < parent.length; i++) {
            int parent = this.parent[i];
            if (!counted.contains(parent)) {
                if (size[parent] > 1) {
                    res++;
                }
                counted.add(parent);
            }
        }

        return res;
    }

    /**
     * Creates file in format: [root_num'delimiter'line]
     *
     * @return path of result file
     */
    private Path createTemporaryFileRootLine() throws TransformerException {
        final var resultPath = Paths.get("").toAbsolutePath().resolve("root_line.txt");

        try (var inputChannel = FileChannel.open(duplicateRemovalInputPath, READ);
             var outputNIOChannel = FileChannel.open(resultPath, TRUNCATE_EXISTING, WRITE, CREATE)) {
            var outputChannel = new OutputChannel(outputNIOChannel);
            int bytesRead;
            int ind = 0;
            var buffer = ByteBuffer.allocate(BUFFER_SIZE);
            var wasRootIndexAppended = false;

            while ((bytesRead = inputChannel.read(buffer)) != -1) {
                var chunk = new ChunkTokenizer(new String(buffer.array(), 0, bytesRead));

                while (chunk.hasRemainingCharacters() && ind <= parent.length) {
                    var newLineInd = chunk.indexOfNewLineFromCurrentIndex();

                    if (!wasRootIndexAppended) {
                        outputChannel.write(parent[ind] + TMP_DELIMITER);
                        ind++;
                        wasRootIndexAppended = true;
                    }
                    if (newLineInd != -1) {
                        // write chunk part before \n (inclusive \n)
                        outputChannel.write(chunk.substring(newLineInd + 1));
                        chunk.setIndex(newLineInd + 1);
                        wasRootIndexAppended = false;
                    } else {
                        outputChannel.write(chunk.substring(chunk.size()));
                        chunk.setIndex(chunk.size());
                    }
                }
                buffer.clear();
            }
        } catch (IOException e) {
            throw new TransformerException("Failed to create temporary file in format: '<group_num>" +
                    TMP_DELIMITER + "<line>'.", e);
        }

        return resultPath;
    }

    private void makeParentsToBeRoots() {
        IntStream.range(0, parent.length).forEach(this::findRoot);
    }

    private void computeDSU() throws TransformerException {
        Path absolutePath = Paths.get("").toAbsolutePath();
        var tmpFilePath = absolutePath.resolve("tmp.txt");
        var sortedTmpFilePath = absolutePath.resolve("tmp_sorted.txt");
        var outputChannelNotClosed = true;
        FileChannel fileOutputChannel = null;

        try (FileChannel fileInputChannel = FileChannel.open(inputFilePath, READ)) {
            fileOutputChannel = FileChannel.open(tmpFilePath, CREATE, WRITE, TRUNCATE_EXISTING);

            var buffer = ByteBuffer.allocate(BUFFER_SIZE);
            int bytesRead;
            int curLineInd = 0;
            var isColumnEnd = true; // to add index for first line
            var columnPartBuffer = ByteBuffer.allocate(BUFFER_SIZE);
            var isFirstTmpLine = true;

            String columnDelimiter = String.valueOf(ChunkTokenizer.getColumnDelimiter());

            while ((bytesRead = fileInputChannel.read(buffer)) != -1) {
                var chunk = new AggregatorChunkTokenizer(new String(buffer.array(), 0, bytesRead));

                while (chunk.hasRemainingCharacters()) {
                    var readColumnState = chunk.accumulateColumnValue(columnPartBuffer);

                    if (columnPartBuffer.position() > 0 && isColumnEnd) {
                        String index;
                        if (isFirstTmpLine) {
                            index = curLineInd + columnDelimiter;
                            isFirstTmpLine = false;
                        } else {
                            index = "\n" + curLineInd + columnDelimiter;
                        }
                        fileOutputChannel.write(ByteBuffer.wrap(index.getBytes()));
                    }

                    columnPartBuffer.flip();
                    while (columnPartBuffer.hasRemaining()) {
                        fileOutputChannel.write(columnPartBuffer);
                    }

                    if (readColumnState == AccumulateValueState.NEW_LINE) {
                        fileOutputChannel.close();
                        outputChannelNotClosed = false;
                        ExternalFileSorter.sortBySecondColumn(tmpFilePath, sortedTmpFilePath);
                        mergeGroupsBySameColumn(sortedTmpFilePath);
                        fileOutputChannel = FileChannel.open(tmpFilePath, CREATE, WRITE, TRUNCATE_EXISTING);
                        outputChannelNotClosed = true;
                        isColumnEnd = true;
                        isFirstTmpLine = true;
                        curLineInd = 0;
                    } else {
                        isColumnEnd = readColumnState == AccumulateValueState.DELIMITER;
                        if (isColumnEnd) {
                            curLineInd++;
                        }
                    }
                    columnPartBuffer.clear();
                }
                buffer.clear();
            }
        } catch (IOException e) {
            throw new TransformerException("Failed while merging groups.", inputFilePath, tmpFilePath, e);
        } finally {
            if (outputChannelNotClosed && fileOutputChannel != null) {
                try {
                    fileOutputChannel.close();
                } catch (IOException e) {
                    System.err.println("Failed to close the file channel: " + e.getMessage());
                }
            }

            try {
                Files.delete(tmpFilePath);
                Files.delete(sortedTmpFilePath);
            } catch (IOException e) {
                System.err.println("Failed to remove temporary files: " + e.getMessage());
            }
        }
    }

    private void mergeGroupsBySameColumn(Path sortedTmpFilePath) throws IOException {
        // assume that column value fits in RAM.
        try (var reader = new BufferedReader(new FileReader(sortedTmpFilePath.toFile()))) {
            String currentLine;
            var previous = IndexValue.from(reader.readLine());

            while ((currentLine = reader.readLine()) != null) {
                var current = IndexValue.from(currentLine);
                if (current.value().equals(previous.value()) && !current.value().equals(EMPTY_VALUE)) {
                    mergeGroups(current.ind(), previous.ind());
                } else {
                    previous = current;
                }
            }
        }
    }

    private record IndexValue(int ind, String value) {
        private static final String DELIMITER = String.valueOf(ChunkTokenizer.getColumnDelimiter());

        public static IndexValue from(String s) {
            var words = s.split(DELIMITER, 2);
            int lineInd = Integer.parseInt(words[0]);
            var columnValue = words[1];

            return new IndexValue(lineInd, columnValue);
        }
    }

    private int findRoot(int ind) {
        if (ind == parent[ind]) {
            return ind;
        }
        parent[ind] = findRoot(parent[ind]);
        return parent[ind];
    }

    private void mergeGroups(int ind1, int ind2) {
        var r1 = findRoot(ind1);
        var r2 = findRoot(ind2);
        if (r1 != r2) {
            if (size[r1] < size[r2]) {
                parent[r1] = r2;
                size[r2] += size[r1];
            } else {
                parent[r2] = r1;
                size[r1] += size[r2];
            }
        }
    }

    private enum AccumulateValueState {
        NEW_LINE, DELIMITER, VALUE_PART
    }

    private static class AggregatorChunkTokenizer extends ChunkTokenizer {

        public AggregatorChunkTokenizer(String s) {
            super(s);
        }

        public AccumulateValueState accumulateColumnValue(ByteBuffer buffer) {
            if (isNewLine(s.charAt(curInd))) {
                curInd++;
                return AccumulateValueState.NEW_LINE;
            }
            if (isColumnDelimiter(s.charAt(curInd))) {
                curInd++;
                return AccumulateValueState.DELIMITER;
            }
            while (hasRemainingCharacters() && !isColumnDelimiter(s.charAt(curInd)) && !isNewLine(s.charAt(curInd))) {
                buffer.put((byte) s.charAt(curInd));
                curInd++;
            }
            if (hasRemainingCharacters() && isColumnDelimiter(s.charAt(curInd))) {
                curInd++;
                return AccumulateValueState.DELIMITER;
            }
            if (hasRemainingCharacters() && isNewLine(s.charAt(curInd))) {
                curInd++;
                return AccumulateValueState.NEW_LINE;
            }

            return AccumulateValueState.VALUE_PART;
        }
    }
}
