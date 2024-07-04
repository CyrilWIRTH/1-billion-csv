package com.decathlon.onebillionrowchallenge.reader;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

public class DataAgg {

    public final Map<String, Data> datas = new HashMap<>(500);

    public static class Data{
        public double min = Double.MAX_VALUE;
        public double max = Double.MIN_VALUE;
        public int cpt = 0;
        public double sum = 0d ;
    }

    public void addData(String key, double value) {
        if (!datas.containsKey(key)) {
            datas.put(key, new Data());
        }

        final var data = datas.get(key);
        data.sum += value;
        data.cpt++;
        if (data.min > value) {
            data.min = value;
        }
        if (data.max < value) {
            data.max = value;
        }
    }



    public void print() {
        datas.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(d -> {
            // var average = new BigDecimal(d.getValue().sum.value.get()).divide(new BigDecimal(d.getValue().cpt.intValue()), 2, RoundingMode.CEILING);
            var average = new BigDecimal(d.getValue().sum).divide(new BigDecimal(d.getValue().cpt), 2, RoundingMode.CEILING);

            System.out.println("store: " + d.getKey() + " min=" + d.getValue().min + " max=" + d.getValue().max + " avg=" + average);
        });
    }


}
