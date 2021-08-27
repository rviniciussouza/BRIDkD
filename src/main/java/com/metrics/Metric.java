package com.metrics;

public interface Metric<T> {
    public double solve(T s, T t);
}
