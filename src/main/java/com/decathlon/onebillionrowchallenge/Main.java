package com.decathlon.onebillionrowchallenge;

import com.decathlon.onebillionrowchallenge.reader.UsingBufferedReader;
import com.decathlon.onebillionrowchallenge.reader.UsingFileChannel;
import com.decathlon.onebillionrowchallenge.reader.UsingScanner;

import java.io.File;
import java.time.Instant;

public class Main {

    public static void main(String[] args) throws Exception {

        if(args.length != 1) {
            System.out.println("Usage: java -jar onebillionrowchallenge.jar <file>");
        }
        final String file = args[0];
        final var start = System.currentTimeMillis();
        System.out.println(Instant.ofEpochMilli(start));
    //    final var reader = new UsingScanner(new File(file));
      //   final var reader = new UsingBufferedReader(new File(file));
        final var reader = new UsingFileChannel(new File(file));

        reader.read();
        reader.print();
        final var end = System.currentTimeMillis();
        System.out.println(Instant.ofEpochMilli(end));
        System.out.println("duration: " + (end - start) + " ms");
    }
}
