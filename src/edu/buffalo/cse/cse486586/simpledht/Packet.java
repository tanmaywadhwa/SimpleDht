package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


public class Packet implements Serializable {
    /**
     * 
     */
    //private static final long serialVersionUID = 1L;
    private static final long serialVersionUID = 123456543L;
    String sendersPort;
    String recieversPort;
    String message;
    String prev;
    String next;
    String key;
    String value;
    String min;
    String max;
    String reply;
    int count;
    Map<Integer,String> indexKey=new HashMap<Integer, String>();
    Map<String,String> keyValue=new HashMap<String, String>();
    public Packet() {
        // TODO Auto-generated constructor stub
    }

}
