package org.example.exceptions;

import java.io.IOException;
import java.nio.file.Path;

public class TransformerException extends IOException {
    public TransformerException(String message) {
        super(message);
    }

    public TransformerException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransformerException(String message, Path input, Path output, Throwable cause) {
        this(message + " Reading " + input.toString() + " and writing to output " + output.toString(), cause);
    }
}
