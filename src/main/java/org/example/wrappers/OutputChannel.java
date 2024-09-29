package org.example.wrappers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public record OutputChannel(FileChannel channel) {

    public int write(String s) throws IOException {
        return channel.write(ByteBuffer.wrap(s.getBytes()));
    }
}
