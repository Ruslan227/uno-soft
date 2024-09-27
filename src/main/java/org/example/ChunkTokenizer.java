package org.example;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.IntStream;

public class ChunkTokenizer {
    private static final char COLUMN_DELIMITER = ';';
    private static final char VALUE_WRAPPER = '\"';
    private final String s;
    private int curInd = 0;

    public ChunkTokenizer(String s) {
        this.s = s;
    }

    public int get() {
        if (hasRemainingCharacters()) {
            return s.charAt(curInd);
        }
        return -1;
    }

    public int getCurrentIndex() {
        return curInd;
    }

    public Optional<Boolean> accumulateColumnValue(ByteBuffer buffer) {
        if (isNewLine(s.charAt(curInd))) {
            return Optional.empty();
        }
        if (isColumnDelimiter(s.charAt(curInd))) {
            curInd++;
            return Optional.of(true);
        }
        while (hasRemainingCharacters() && !isColumnDelimiter(s.charAt(curInd)) && !isNewLine(s.charAt(curInd))) {
            buffer.put((byte) s.charAt(curInd));
            curInd++;
        }
        if (hasRemainingCharacters() && isColumnDelimiter(s.charAt(curInd))) {
            curInd++;
            return Optional.of(true);
        }

        return Optional.of(hasRemainingCharacters() && isNewLine(s.charAt(curInd)));
    }

    /**
     * @return position of last new line if new valid line was reached. If chunk was ended and not all new line symbols was
     * skipped then return -1.
     */
    public int skipUtilNewValidLine() {
        if (!hasRemainingCharacters()) {
            return -1;
        }

        while (hasRemainingCharacters()) {
            if (isNewLine(s.charAt(curInd))) {
                if (!skipNewLines()) {
                    return -1;
                } else {
                    return curInd - 1;
                }
            }
            skipColumnDelimiter();
            skipDigits();
            skipValueWrapper();
        }

        return -1;
    }

    /**
     * @return true if column delimiter is going next.
     */
    public boolean skipColumnDelimiter() {
        return abstractSkip(this::isColumnDelimiter, () -> curInd++);
    }

    /**
     * @return true if value wrapper is going next.
     */
    public boolean skipValueWrapper() {
        return abstractSkip(this::isValueWrapper, () -> curInd++);
    }

    /**
     * @return true if new lines are going next.
     */
    public boolean skipNewLines() {
        return abstractSkip(this::isNewLine, this::skipNewLinesIfPresent);
    }

    /**
     * @return true if digits are going next.
     */
    public boolean skipDigits() {
        return abstractSkip(this::isDigit, this::skipDigitsIfPresent);
    }

    private boolean abstractSkip(Predicate<Character> predicate, Runnable skipFunction) {
        skipWhitespaceIfPresent();

        if (hasRemainingCharacters() && predicate.test(s.charAt(curInd))) {
            skipFunction.run();
            return true;
        }

        return false;
    }

    public void skipWhitespaceIfPresent() {
        abstractSkipLexemesIfPresent(this::isWhitespace);
    }

    private void skipDigitsIfPresent() {
        abstractSkipLexemesIfPresent(this::isDigit);
    }

    private void skipNewLinesIfPresent() {
        abstractSkipLexemesIfPresent(this::isNewLine);
    }

    private void abstractSkipLexemesIfPresent(Predicate<Character> predicate) {
        for (; hasRemainingCharacters() && predicate.test(s.charAt(curInd)); curInd++) {
        }
    }

    public boolean hasRemainingCharacters() {
        return curInd != s.length();
    }

    public long countColumnDelimitersWithRightBorder(int endIndex) {
        return countByPredicateWithRightBorder(this::isColumnDelimiter, endIndex);
    }

    public long countColumnDelimiters() {
        return countByPredicateWithRightBorder(this::isColumnDelimiter, s.length());
    }

    public long countByPredicateWithRightBorder(Predicate<Character> predicate, int endIndex) {
        return IntStream.range(0, endIndex)
                .filter(i -> predicate.test(s.charAt(i)))
                .count();
    }

    public int indexOfNewLine() {
        return indexOf(this::isNewLine);
    }

    public int indexOf(Predicate<Character> predicate) {
        return IntStream.range(0, s.length())
                .filter(i -> predicate.test(s.charAt(i)))
                .findFirst()
                .orElse(-1);
    }

    public boolean isNewLine(char c) {
        return c == '\n' || c == '\r';
    }

    public boolean isColumnDelimiter(char c) {
        return c == COLUMN_DELIMITER;
    }

    public boolean isValueWrapper(char c) {
        return c == VALUE_WRAPPER;
    }

    public boolean isDigit(char c) {
        return Character.isDigit(c);
    }

    public boolean isWhitespace(char c) {
        return !isNewLine(c) && Character.isWhitespace(c);
    }
}
