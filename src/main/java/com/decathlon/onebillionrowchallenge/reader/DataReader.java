package com.decathlon.onebillionrowchallenge.reader;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public abstract class DataReader {

    public static class Data {
        private Double min;
        private Double max;
        private AtomicInteger cpt = new AtomicInteger(0);
        private AtomicDouble sum = new AtomicDouble();
    }

    static class AtomicDouble {
        private AtomicReference<Double> value = new AtomicReference(Double.valueOf(0.0));
        double getAndAdd(double delta) {
            while (true) {
                Double currentValue = value.get();
                Double newValue = Double.valueOf(currentValue.doubleValue() + delta);
                if (value.compareAndSet(currentValue, newValue))
                    return currentValue.doubleValue();
            }
        }
    }

    protected final Map<String, Data> datas = new ConcurrentHashMap<>();

    public abstract void read() throws Exception;

    public void addData(String key, double value) {
        if (!datas.containsKey(key)) {
            datas.put(key, new Data());
        }

        final var data = datas.get(key);
        data.sum.getAndAdd(value);
        data.cpt.incrementAndGet();
        if (data.min == null || data.min > value) {
            data.min = value;
        }
        if (data.max == null || data.max < value) {
            data.max = value;
        }
    }

    public void print() {
        datas.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(d -> {
             var average = new BigDecimal(d.getValue().sum.value.get()).divide(new BigDecimal(d.getValue().cpt.intValue()), 2, RoundingMode.CEILING);

            System.out.println("store: " + d.getKey() + " min=" + d.getValue().min + " max=" + d.getValue().max + " avg=" + average);
        });
    }

}
