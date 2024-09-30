package org.example.exceptions;

import java.io.IOException;
import java.nio.file.Path;

public class ExternalSortException extends IOException {
    public ExternalSortException(String message) {
        super(message);
    }

    public ExternalSortException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExternalSortException(String message, Path input, Throwable cause) {
        super(message + " Input file: " + input.toString(), cause);
    }
}
