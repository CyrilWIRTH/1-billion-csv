package com.decathlon.onebillionrowchallenge.reader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class UsingFileChannel extends DataReader {

    private FileChannel fileChannel;
    private Long chunkSize;
    private int numberOfThreads;

    public UsingFileChannel(File file) throws IOException {
        fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        numberOfThreads = Runtime.getRuntime().availableProcessors() * 1000;
        chunkSize = fileChannel.size() / numberOfThreads;
    }

    private int backUntilEOL(long position) throws IOException {
        long lastPosition = position;

        byte[] data = new byte[1];

        var t = fileChannel.map(FileChannel.MapMode.READ_ONLY, Long.max(lastPosition - 1, 0), 1);
        t.get(data);
        if (data[0] == '\n') {
            return 0;
        }

        int total = 0;

        int localChunk = 50;
        while (true) {
            final var buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, lastPosition - localChunk, localChunk);
            while (buffer.hasRemaining()) {
                data = new byte[buffer.remaining()];
                buffer.get(data);
                for (int i = data.length - 1; i >= 0; i--) {
                    total++;
                    if (data[i] == '\n') {
                        return total;
                    }
                }
            }
            lastPosition -= localChunk;
        }
    }

    @Override
    public void read() throws IOException, InterruptedException {
        List<Thread> threads = new ArrayList<>();
        List<CounterThread> runnables = new ArrayList<>();

        long position = 0;
        for (int i = 0; i < numberOfThreads; i++) {
            if (position > fileChannel.size()) {
                break;
            }

            var buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, chunkSize);
            final var backUntilEOL = backUntilEOL(position + chunkSize);

            position += (chunkSize - backUntilEOL);

            var runnable = new CounterThread(buffer, backUntilEOL);
            runnables.add(runnable);
            Thread counterThread = Thread.startVirtualThread(runnable);
            threads.add(counterThread);
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }

    class CounterThread implements Runnable {


        private ByteBuffer buffer;
        private final int minusUntilLastEOL;


        public CounterThread(ByteBuffer buffer, int minusUntilLastEOL) {
            this.buffer = buffer;
            this.minusUntilLastEOL = minusUntilLastEOL;
        }

        @Override
        public void run() {

            final var readUntil = chunkSize - minusUntilLastEOL;

            boolean skip = false;
            int alreadyRead = 0;
            while (buffer.hasRemaining()) {
                if(skip) {
                    break;
                }
                int remaining = buffer.remaining();

                if (alreadyRead + remaining > readUntil) {
                    remaining =  ((Long)(readUntil - alreadyRead)).intValue();
                    skip = true;
                }

                alreadyRead += remaining;

                byte[] data = new byte[remaining];
                buffer.get(data);

                final var bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data), StandardCharsets.UTF_8));

                
                String line;
                try {
                    line = bufferedReader.readLine();
                    while (true) {
                        String currentLine = line;
                        if (!((line = bufferedReader.readLine()) != null)) {
                            break;
                        }
                        check(currentLine);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        }

        private void check(String line) {
            if (line.isEmpty()) {
                return;
            }
            var d = line.split(SEPARATOR);
            final var currentValue = Double.valueOf(d[1]);
            addData(d[0], currentValue);
        }
    }

    static final String SEPARATOR = ";";

}
