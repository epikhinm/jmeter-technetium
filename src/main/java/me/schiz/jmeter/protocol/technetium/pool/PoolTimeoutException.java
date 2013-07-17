package me.schiz.jmeter.protocol.technetium.pool;

public class PoolTimeoutException extends Exception {
    public String toString() {
        return "Pool timed out";
    }
}
