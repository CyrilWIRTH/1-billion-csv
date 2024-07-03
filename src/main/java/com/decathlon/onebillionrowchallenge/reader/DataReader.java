package com.decathlon.onebillionrowchallenge.reader;

public abstract class DataReader {

    private Double min = Double.MAX_VALUE;
    private Double max = Double.MIN_VALUE;

    public abstract void read() throws Exception;

    public Double getMin() {return min;}
    public Double getMax() {return max;}

    protected void replaceMinIfNeeded(Double value) {
        if(value < min) {
            this.min = value;
        }
    }
    protected void replaceMaxIfNeeded(Double value) {
        if(value > max) {
            this.max = value;
        }
    }

}
