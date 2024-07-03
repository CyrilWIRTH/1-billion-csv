package com.decathlon.onebillionrowchallenge.reader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UsingFileChannel extends DataReader {

    private FileChannel fileChannel;
    private long chunkSize;
    private int numberOfThreads;

    public UsingFileChannel(File file) throws IOException {
        fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        numberOfThreads = Runtime.getRuntime().availableProcessors() * 1000;
        chunkSize = fileChannel.size() / numberOfThreads;
    }

    private byte[] completeUntilEOL(long position) throws IOException {
        long lastPosition = position;

        byte[] data = new byte[1];

        var t = fileChannel.map(FileChannel.MapMode.READ_ONLY, Long.max(lastPosition - 1, 0), 1);
        t.get(data);
        if (data[0] == '\n') {
            return null;
        }

        List<byte[]> tmp = new ArrayList<>();
        int localChunk = 50;
        while (true) {
            final var buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, lastPosition, localChunk);
            while (buffer.hasRemaining()) {
                data = new byte[buffer.remaining()];
                buffer.get(data);
                for (int i = 0; i < data.length; i++) {
                    if (data[i] == '\n') {
                        tmp.add(Arrays.copyOfRange(data, 0, i));

                        byte[] lastOne = new byte[localChunk * (tmp.size() - 1) + tmp.getLast().length];

                        for (int j = 0; j < tmp.size(); j++) {
                            System.arraycopy(tmp.get(j), 0, lastOne, localChunk * j, tmp.get(j).length);
                        }

                        return lastOne;
                    }

                }
                tmp.add(data);
            }
            lastPosition += localChunk;
        }
    }

    @Override
    public void read() throws IOException, InterruptedException {


        List<Thread> threads = new ArrayList<>();
        List<CounterThread> runnables = new ArrayList<>();

        long position = 0;
        for (int i = 0; i < numberOfThreads; i++) {
            if(position > fileChannel.size()) {
                break;
            }

            if(position+ chunkSize > fileChannel.size()) {
                chunkSize = fileChannel.size() - position;
            }

            var buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, chunkSize);
            final var plus = completeUntilEOL(position + chunkSize);

            position += (chunkSize + (plus == null ? 0 : plus.length));
            System.out.println(position);
            var runnable = new CounterThread(buffer, plus);
            runnables.add(runnable);
            Thread counterThread = Thread.startVirtualThread(runnable);
            threads.add(counterThread);
        }
        for (Thread thread : threads) {
            thread.join();
        }
        for (CounterThread runnable : runnables) {
            replaceMaxIfNeeded(runnable.getMax());
            replaceMinIfNeeded(runnable.getMin());
        }
    }

    class CounterThread extends Thread {
        private Double localMin = Double.MAX_VALUE;
        private Double localMax = Double.MIN_VALUE;


        private ByteBuffer buffer;
        private final byte[] plus;


        public CounterThread(ByteBuffer buffer, byte[] plus) {
            this.buffer = buffer;
            this.plus = plus;
        }

        @Override
        public void run() {
            String lastLine = null;
            while (buffer.hasRemaining()) {
                byte[] data = new byte[buffer.remaining()];
                buffer.get(data);

                final var bufferedReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data)));

                String line;

                try {
                    line = bufferedReader.readLine();

                    while (true) {
                        String currentLine = line;
                        if (!((line = bufferedReader.readLine()) != null)) {
                            if (plus != null) {
                                check(currentLine + new String(plus));
                            }
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
            if(line.isEmpty()) {
                return;
            }
            final var currentValue = Double.valueOf(line.split(";")[1]);
            replaceMinIfNeeded(currentValue);
            replaceMaxIfNeeded(currentValue);
        }

        public Double getMin() {
            return localMin;
        }

        public Double getMax() {
            return localMax;
        }

        protected void replaceMinIfNeeded(Double value) {
            if (value < localMin) {
                this.localMin = value;
            }
        }

        protected void replaceMaxIfNeeded(Double value) {
            if (value > localMax) {
                this.localMax = value;
            }
        }

    }
}
