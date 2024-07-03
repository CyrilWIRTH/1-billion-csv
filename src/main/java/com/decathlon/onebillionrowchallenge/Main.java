package com.decathlon.onebillionrowchallenge;

import com.decathlon.onebillionrowchallenge.reader.UsingBufferedReader;
import com.decathlon.onebillionrowchallenge.reader.UsingFileChannel;
import com.decathlon.onebillionrowchallenge.reader.UsingScanner;

import java.io.File;

public class Main {

    public static void main(String[] args) throws Exception {

        final String file = "/Users/CYRIL/Downloads/measurements.txt";
        // final var reader = new UsingScanner(new File(file));
        // final var reader = new UsingBufferedReader(new File(file));
        final var reader = new UsingFileChannel(new File(file));
        final var start = System.currentTimeMillis();
        reader.read();
        final var end = System.currentTimeMillis();

        System.out.println("min: " + reader.getMin() + " max: " + reader.getMax());
        System.out.println("duration: " + (end - start) + " ms");
    }
}
