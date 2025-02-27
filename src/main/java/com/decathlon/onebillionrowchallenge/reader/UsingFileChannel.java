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
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class UsingFileChannel extends DataReader {

    private FileChannel fileChannel;
    private long chunkSize;
    private int numberOfThreads;

    public UsingFileChannel(File file) throws IOException {
        fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        numberOfThreads = Runtime.getRuntime().availableProcessors() * 1000;
        chunkSize = fileChannel.size() / numberOfThreads;
    }

    private int backUntilEOL(long position) throws IOException {
        long lastPosition = position;

        int total = 0;

        int localChunk = 50;
        while (true) {
            final var buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, lastPosition - localChunk, localChunk);
            while (buffer.hasRemaining()) {
                var data = new byte[buffer.remaining()];
                buffer.get(data);
                for (int i = data.length - 1; i >= 0; i--) {
                    if (data[i] == '\n') {
                        return total;
                    }
                    total++;
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

        List<CounterThread> all = new ArrayList<>();
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

        runnables.forEach(r -> {
            r.datas.forEach((k, v) -> {
              final var globalData = UsingFileChannel.this.datas;

              if(!globalData.containsKey(k)) {
                  globalData.put(k, new Data());
              }
              final var d = globalData.get(k);

              d.cpt += v.cpt;
              d.sum += v.sum;
              d.min = Math.min(d.min, v.min);
              d.max = Math.max(d.max, v.max);

            });
        });

    }

    class CounterThread extends DataAgg
            implements Runnable {


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
                    remaining =  ((int)(readUntil - alreadyRead));
                    skip = true;
                }

                alreadyRead += remaining;

                byte[] data = new byte[remaining];
                buffer.get(data);

                //readWithReader(data);
             //   readWithMemory(data);
                readWithMemoryTokenizer(data);
            }
        }

        private void readWithMemoryTokenizer(byte[] data) {
            final var str = new String(data);
            final var tokenizer = new StringTokenizer(str, "\n");
            while(tokenizer.hasMoreTokens()) {
                check(tokenizer.nextToken());
            }
        }

        private void readWithMemory(byte[] data) {
            final var str = new String(data);
            var lines = str.split("\n");
            for(int i = 0; i < lines.length; i++) {
                check(lines[i]);
            }
        }

        private void readWithReader(byte[] data) {
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

        private void check(String line) {
            if (line.isEmpty()) {
                return;
            }
            var d = split(line);

            final var currentValue = Double.parseDouble(d[1]);
            addData(d[0], currentValue);
        }


        private String[] split(String line) {
            final var tokenizer = new StringTokenizer(line, SEPARATOR);
            var first = tokenizer.nextToken();
            return new String[]{first, tokenizer.nextToken()};
        }

        /*
        private String[] split(String line) {
            var tab = line.toCharArray();
            int index = 0;
            for(int i = tab.length - 1; i >= 0; i--) {
                if(tab[i] == ';') {
                    index = i;
                    break;
                }
            }

            var a = new char[index];
            var b = new char[tab.length - index - 1];

            System.arraycopy(tab, 0, a, 0, index);
            System.arraycopy(tab, index + 1, b, 0, tab.length - index - 1);

            return new String[]{new String(a), new String(b)};
        }

         */
    }

    static final String SEPARATOR = ";";

}
