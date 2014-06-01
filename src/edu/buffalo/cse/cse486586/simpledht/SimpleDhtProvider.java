package edu.buffalo.cse.cse486586.simpledht;
import edu.buffalo.cse.cse486586.simpledht.Packet;
import edu.buffalo.cse.cse486586.simpledht.Star;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    public static final int SERVER_PORT=10000;
    String successor=new String();
    String predecessor=new String();
    int nodeCount=1;
    String nodeMin=new String();
    String nodeMax=new String();
    static String myPort=new String();;
    static boolean Reply=false;
    static boolean starReply=false;
    String response=new String();
    Star starResponse=new Star();
    Functions func=new Functions();
    ArrayList<String> network=new ArrayList<String>();
    Map<String,String> HashWithRealPorts;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if(selection.equals("@")){
            String[] savFiles = getContext().fileList();
            for (int i = 0; i < savFiles.length; i++) {
                func.del(savFiles[i]);
            }           
            return 0;
        }
        else if(selection.equals("*")){
            String[] savFiles = getContext().fileList();
            for (int i = 0; i < savFiles.length; i++) {
                func.del(savFiles[i]);
            }
            Packet Outgoing=new Packet();
            Outgoing.message="delete";
            Outgoing.sendersPort=myPort;
            Outgoing.recieversPort=successor;
            Outgoing.key="*";
            new Client().execute(Outgoing);
            return 0;
        }
        else{
            func.del(selection);

        }
        return 0;
    }
    @Override
    public String getType(Uri uri) {
        return null;
    }
    @Override

    public Uri insert(Uri uri, ContentValues values) {

        if(Lookup(values.getAsString("key")) ==1){
            func.Ins(values.getAsString("key"),values.getAsString("value"));
            return uri;
        }
        {
            Packet Outgoing=new Packet();
            Outgoing.message="insert";
            Outgoing.key=values.getAsString("key");
            Outgoing.value=values.getAsString("value");
            Outgoing.recieversPort=successor;
            Outgoing.sendersPort=myPort;
            new Client().execute(Outgoing);
            return uri;
        }
    }
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        MatrixCursor mc = new MatrixCursor(new String[]{"key","value"});

        if(selection.equals("@"))        
        {
            String[] savFiles = getContext().fileList();
            for (int i = 0; i < savFiles.length; i++) {
                selection=savFiles[i];
                String column[] = new String[2];
                column[0] = selection;
                column[1] = func.qry(selection);

                mc.addRow(column);
            }
            return mc;
        }
        if(selection.equals("*") && nodeCount==1)        
        {
            String[] savFiles = getContext().fileList();
            for (int i = 0; i < savFiles.length; i++) {
                selection=savFiles[i];
                String column[] = new String[2];
                column[0] = selection;
                column[1] = func.qry(selection);

                mc.addRow(column);
            }
            return mc;
        }

        else if(selection.equals("*") && nodeCount>1)
        {   
            String[] savFiles = getContext().fileList();
            Packet starRequestPacket=new Packet();
            starRequestPacket.message="starQuery";
            starRequestPacket.sendersPort=myPort;
            starRequestPacket.recieversPort=successor;
            String localKey=new String();
            for (int i = 0; i < savFiles.length; i++) {
                localKey=savFiles[i];
                starRequestPacket.indexKey.put(i, localKey);
                starRequestPacket.keyValue.put(localKey, func.qry(localKey));
            }try{
                Log.i("Sending *Packet from ",myPort);
                new Client().execute(starRequestPacket);
            }
            catch(Exception e){
                Log.e("Error while sending ","star packet");
                e.printStackTrace();
            }
            while(!starReply){}

            for(int i=0;i<starResponse.indexKey.size();i++)  {
                String column[] = new String[2];
                String temp=column[0]= starResponse.indexKey.get(i);
                column[1]= starResponse.keyValue.get(temp);
                mc.addRow(column);

            }
            return mc;
        }
        else{
            if(Lookup(selection)==1) {
                String column[]=new String[2];
                column[0]=selection;
                column[1]=func.qry(selection);
                mc.addRow(column);
                return mc;
            }
            else if(Lookup(selection)==0){
                Packet Outgoing=new Packet();
                Outgoing.message="query";
                Outgoing.key=selection;
                Outgoing.recieversPort=successor;
                Outgoing.sendersPort=myPort;
                //new Client().execute(Outgoing);
                new Client().doInBackground(Outgoing);
                Log.i("Entering endless while","Waiting");  
                while(Reply==false){}
                String column[]=new String[2];
                column[0]=selection;
                column[1]=response;
                mc.addRow(column);
                Reply=false;
                return mc;
            }
            return mc;
        }
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {

        return 0;
    }

    @Override
    public boolean onCreate() {
        // Finding out the current AVD port.

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        myPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        Log.i("myPort",myPort);

        if (myPort.equals("5554")){
            Log.i("Declaration", "I am 5554");
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new Server().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

            } catch (IOException e) {
                Log.e("Server", "Can't create the 5554 ServerSocket");
                e.printStackTrace();
                return true;
            }    
        }
        else {
            Log.i("Declaration", "I am "+myPort);
            try {
                ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
                new Server().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            } catch (IOException e) {
                Log.e("Server", "Can't create an others ServerSocket");
                e.printStackTrace();
            }
            Packet Request = new Packet();
            Request.message="connectRequest";
            Request.sendersPort=myPort;
            Request.recieversPort="5554";
            //new Client().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, Request);
            new Client().execute(Request);
            Log.i(myPort,"Notified 5554");
            return true;
        }
        return false;
    }


    //////////////////////////Server Thread////////////////////////////////////

    public class Server extends AsyncTask<ServerSocket, Packet, Void>
    {
        String Predecessor=new String();
        String Successor=new String();


        protected Void doInBackground(ServerSocket... params) {
            if(myPort.equals("5554")){

                HashWithRealPorts=new HashMap<String,String>();
                //Packet Outgoing;
                try{
                    String networkEntry=new String();
                    networkEntry=genHash("5554");
                    String[] remotePorts={"5554","5556","5558","5560","5562"};
                    network.add(networkEntry);
                    for(String s:remotePorts){
                        Log.d("hashmap", s);
                        HashWithRealPorts.put(genHash(s),s);   
                    }
                }
                catch(Exception e){
                    Log.e("got stuck while creating hash table", "when port is 5554");
                    e.printStackTrace();
                }
                Log.i("Added Hash elements","Server 5554");
            }

            try{
                ServerSocket serverSocket = params[0];
                while(true){
                    Socket accept= serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(accept.getInputStream());
                    Packet Incoming=new Packet();
                    Incoming=(Packet) in.readObject();

                    if (myPort.equals("5554") && Incoming.message.equals("connectRequest")){
                        network.add(genHash(Incoming.sendersPort));
                        Collections.sort(network);
                        for(int cur=0;cur<network.size();cur++){
                            if(cur==0 && network.size()>1){
                                Predecessor=HashWithRealPorts.get(network.get(network.size()-1));
                                Successor=HashWithRealPorts.get(network.get(cur+1));
                            }
                            else if(cur>0 && cur<network.size()-1){
                                Predecessor=HashWithRealPorts.get(network.get(cur-1));
                                Successor=HashWithRealPorts.get(network.get(cur+1));
                            }
                            else if(cur==network.size()-1){
                                Predecessor=HashWithRealPorts.get(network.get(cur-1));
                                Successor=HashWithRealPorts.get(network.get(0));
                            }

                            Packet Outgoing;
                            Outgoing=new Packet();
                            Outgoing.message="connectAck";
                            Outgoing.next=Successor;
                            Outgoing.prev=Predecessor;
                            Outgoing.min=HashWithRealPorts.get(network.get(0));
                            Outgoing.max=HashWithRealPorts.get(network.get(network.size()-1));
                            Outgoing.sendersPort=myPort;
                            Outgoing.recieversPort=HashWithRealPorts.get(network.get(cur)); //HashWithRealPorts.
                            Outgoing.count=network.size();
                            new Client().execute(Outgoing);
                            Log.i(myPort,"New predecessor and successor sent to"+HashWithRealPorts.get(network.get(cur)));//
                        }
                    }


                    else if(Incoming.message.equals("connectAck")){
                        successor=Incoming.next;
                        predecessor=Incoming.prev;
                        Log.e("Predicessor",predecessor);
                        Log.e("Successor",successor);
                        nodeMin=Incoming.min;
                        nodeMax=Incoming.max;
                        nodeCount=Incoming.count;
                        Log.i("connectAck","pred:"+predecessor+" succ: "+successor);
                        Log.i("connectAck","min:"+nodeMin+" max: "+nodeMax);
                    }
                    else if(Incoming.message.equals("insert")){
                        Log.d("Inside Insert","calling Lookup");
                        if(Lookup(Incoming.key)==1){
                            func.Ins(Incoming.key,Incoming.value);                            
                        }
                        else if(Lookup(Incoming.key)==0 && !Incoming.sendersPort.equals(myPort)){
                            Log.d("Lookup Failed","Forwarding node to "+successor);
                            Incoming.recieversPort=successor;
                            new Client().execute(Incoming);
                        }
                    }
                    else if(Incoming.message.equals("delete")){
                        if(Incoming.key.equals("*") && !Incoming.sendersPort.equals(myPort)){
                            String[] savFiles = getContext().fileList();
                            for (int i = 0; i < savFiles.length; i++) {
                                func.del(savFiles[i]);
                            }
                            Packet Outgoing=new Packet();
                            Outgoing=Incoming;
                            Outgoing.recieversPort=successor;
                            new Client().execute(Outgoing);
                        }

                        else if(Lookup(Incoming.key)==1){
                            func.del(Incoming.key);                            
                        }

                        else if(Lookup(Incoming.key)==0){
                            if(Incoming.sendersPort != myPort){
                                Packet Outgoing=new Packet();
                                Outgoing=Incoming;
                                Outgoing.recieversPort=successor;
                                new Client().execute(Outgoing);
                            }
                        }
                    }
                    else if(Incoming.message.equals("query")){

                        if(Lookup(Incoming.key)==1){
                            Packet Outgoing;
                            Outgoing=new Packet();
                            Outgoing.message="reply";
                            Outgoing.reply=func.qry(Incoming.key);
                            Outgoing.sendersPort=myPort;
                            Outgoing.recieversPort=Incoming.sendersPort;
                            new Client().execute(Outgoing);
                        }
                        else if(Lookup(Incoming.key)==0 && !Incoming.sendersPort.equals(myPort)){
                            Incoming.recieversPort=successor;
                            Log.e("successor",Incoming.recieversPort=successor);
                            Log.e("Sender's port",Incoming.sendersPort);
                            new Client().execute(Incoming);

                        }
                    }
                    else if(Incoming.message.equals("starQuery")){
                        try{
                            if(!Incoming.sendersPort.equals(myPort)){
                                String selection=new String();
                                String[] savFiles = getContext().fileList();
                                int size=Incoming.indexKey.size();
                                for (int i = 0; i < savFiles.length; i++) {
                                    selection=savFiles[i];
                                    Incoming.indexKey.put(size+i, selection);
                                    Incoming.keyValue.put(selection, func.qry(selection));
                                }
                                Log.i(myPort+" added info to *Packet and forwarding to",successor);
                                Incoming.recieversPort=successor;
                                new Client().execute(Incoming);
                            }

                            else if(Incoming.sendersPort.equals(myPort)){
                                starResponse.indexKey=Incoming.indexKey;
                                starResponse.keyValue=Incoming.keyValue;
                                starReply=true;    
                            }
                        }
                        catch(Exception e){
                            Log.e("Exception while adding to ","starQuery packet" );
                            e.printStackTrace();
                        }
                    }

                    else if(Incoming.message.equals("reply")){
                        //String Value=new String();
                        response=Incoming.reply;
                        Reply=true;
                    }
                    accept.close();
                }
            } 
            catch(Exception e){
                Log.e("Server", "Problem while recieving");
                e.printStackTrace();
            }
            return null;
        }

    }
    ////////////////////////Client///////////////////////////   
    public class Client extends AsyncTask<Packet, Void , Void>{

        @Override
        protected Void doInBackground(Packet... params) {

            try{
                //Log.e("Sending",params[0].recieversPort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        (Integer.parseInt(params[0].recieversPort)*2));
                Log.i("sendingTo",(Integer.parseInt(params[0].recieversPort)*2)+"");

                ObjectOutputStream out=new ObjectOutputStream(socket.getOutputStream());
                Object temp=params[0];
                out.writeObject(temp);
                out.flush();
                out.close();
                socket.close();
                Log.i(myPort,"Message Sent");
            }
            catch( Exception e){
                Log.e("Error sending from Client",myPort);
                e.printStackTrace();
            }
            return null;
        }
    }

///////////////// data modification functions//////////////////////////////
    public class Functions
    {
        public int del(String key){
            getContext().deleteFile(key);
            return 0;
        }
        public void Ins(String key, String value){
            String Filename = key;
            String hashedFilename=null;
            //Converting Key into Hash. 
            try {
                Log.e("Write unhashedFilename", Filename );
                hashedFilename= genHash(Filename);
                Log.e("Write filename", hashedFilename );
            } catch (NoSuchAlgorithmException e1) {
                e1.printStackTrace();
            }
            String string = value;
            Log.e("Write string", string );
            File Location = new File(getContext().getFilesDir().getAbsolutePath());

            try {
                File fout=new File(Location+"/"+Filename);
                FileOutputStream outputStream = new FileOutputStream(fout);
                //Log.v(Filename+" Write", string);
                outputStream.write(string.getBytes());
                outputStream.close();

            } catch (Exception e) {
                Log.e("File not written", Filename);
                Log.e("Stacktrace",e.getMessage());
            }
        }


        //////////Client Query///////////
        public String qry(String selection){
            //MatrixCursor mc = new MatrixCursor(new String[]{"key","value"});
            File Location = new File(getContext().getFilesDir().getAbsolutePath());
            BufferedReader br = null;
            StringBuilder sb = new StringBuilder();
            String column[] = new String[2];
            column[0] = selection;
            Log.e("Read filename", selection );
            try{
                File fin=new File(Location+"/"+(selection));
                FileInputStream inputStream = new FileInputStream(fin);
                br=new BufferedReader(new InputStreamReader(inputStream));
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            }
            catch(Exception e){
                Log.e("error while reading or Hashing filename",e.getMessage());
            }
            finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            Log.e("Read Value", sb.toString());
            return sb.toString();
        }
    }
    private String genHash(String input) throws NoSuchAlgorithmException {
        Formatter formatter=null;

        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        String temp=formatter.toString();
        formatter.close();
        return temp;
    }

    private int Lookup(String check){
        String key=new String();
        key=check;
        try{
            Log.e("in Lookup",myPort);
            if(nodeCount==1){
                Log.i("Lookup","Returning 1 since nodeCount=1");
                return 1;
            }
            else if(!myPort.equals(nodeMin)){
                if(genHash(predecessor).compareTo(genHash(key)) < 0 && genHash(myPort).compareTo(genHash(key))>=0){
                    Log.i("Lookup","Returning 1 at myPort!=nodeMin");
                    return 1;
                }
                else {
                    Log.i("Lookup","Returning 0 at myPort!=nodeMin");
                    return 0;
                }

            }
            else if(myPort.equals(nodeMin)){
                if(genHash(myPort).compareTo(genHash(key))>0 ||genHash(nodeMax).compareTo(genHash(key))<0 ){
                    Log.i("Lookup","Returning 1 at myPort==nodeMin");
                    return 1;
                }
                else{
                    Log.i("Lookup","Returning 0 at myPort==nodeMin");
                    return 0;
                }
            }
            else{
                Log.d("Reached the unreachabe","in Lookup");
                return 0;
            }

        }
        catch(Exception e){
            Log.e("Exception in lookup","see details below");
            e.printStackTrace();
            return 0;

        }

    }
}

