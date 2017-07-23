package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yifan on 4/24/17.
 */

public class ConcurrentMsg {
    public Map<String, ArrayList<String>> version;
    private String key;
    private String value;
    private int counter;
    public ConcurrentMsg() {
        version = new ConcurrentHashMap<String, ArrayList<String>>();
        this.key = "";
        this.value = "";
        counter = 0;
    }
    public ConcurrentMsg(String key, String value, int counter) {
        this.key = key;
        this.value = value;
        this.counter = counter;
    }
    public void setKey(String key) {
        this.key = key;
    }
    public void setValue(String value) {
        this.value = value;
    }
    public void setCounter(int counter) {
        this.counter = counter;
    }
    public String getKey() {
        return this.key;
    }
    public String getValue() {return this.value;}
    public int getCounter() {
        return this.counter;
    }
}
