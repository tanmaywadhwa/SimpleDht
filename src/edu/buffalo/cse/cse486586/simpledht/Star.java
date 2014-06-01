package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Star implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = -1418356767059888657L;
    Map<Integer,String> indexKey=new HashMap<Integer, String>();
    Map<String,String> keyValue=new HashMap<String, String>();
    
    public Star(){
        
    }

}
