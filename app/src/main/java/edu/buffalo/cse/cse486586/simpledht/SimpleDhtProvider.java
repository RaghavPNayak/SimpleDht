package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.renderscript.Sampler;
import android.telephony.TelephonyManager;
import android.text.method.KeyListener;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final Uri Content_URI = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
    public String myPort;
    public int ServerPort = 10000;
    public TreeMap<String, String> sortedHashMap = new TreeMap<String, String>();
    public HashMap<String, String > allFilesStored = new HashMap<String, String>();
    HashMap<String,String > temp1 = new HashMap<String ,String>();
    ArrayList<String >KeysListMain = new ArrayList<String>();
    ArrayList<String  >ValuesListMain = new ArrayList<String>();
    ArrayList<String >FinalKeysList = new ArrayList<String>();
    ArrayList<String >FinalValsList = new ArrayList<String>();


    String hashPort;
    String pred = null;
    String suc = null;
    String predPortNo = null;
    String sucPortNo = null;
    String returnKey;
    String returnValue;
    String returnType=null;
    boolean waitFlag = true;
    String chordsize = null;
    int count =1;
    String portStr;
    String CheckFlag = "false";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        if (!selection.equals("@") || !selection.equals("*")){
            if (pred==null){
                getContext().deleteFile(selection);
            }
            else if (pred!=null){
                String key = selection;
                try {
                    String keyHash = genHash(selection);
                    if (keyHash.compareTo(pred) < 0 && keyHash.compareTo(hashPort) <= 0 && pred.compareTo(hashPort) > 0) {
                        getContext().deleteFile(selection);
                    } else if (keyHash.compareTo(pred) > 0 && keyHash.compareTo(hashPort) > 0 && pred.compareTo(hashPort) > 0) {
                        getContext().deleteFile(selection);
                    } else if (keyHash.compareTo(pred) > 0 && keyHash.compareTo(hashPort) < 0) {
                        getContext().deleteFile(selection);
                    }
                    else {
                        String deleteFile = sucPortNo+"#"+"forwardDelete"+selection;
                        forwardDelete(deleteFile);
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
        }else if (selection.equals("@")){
            File directory = getContext().getFilesDir();
            String[] dir = directory.list();
            for (String s : dir) {
                Log.i("Deleting File", s);
                getContext().deleteFile(s);
            }
        }
        else if (selection.equals("*")){
            if (pred==null){
                File directory = getContext().getFilesDir();
                String[] dir = directory.list();
                for (String s : dir) {
                    Log.i("Deleting File", s);
                    getContext().deleteFile(s);
                }
            }else if (pred!=null) {
                int timeToBreak = 1;
                while (timeToBreak < 2) {

                    File directory = getContext().getFilesDir();
                    String[] dir = directory.list();
                    for (String s : dir) {
                        Log.i("Deleting File", s);
                        getContext().deleteFile(s);
                    }
                    String deleteFile = sucPortNo+"#"+"forwardDelete"+selection;
                    forwardDelete(deleteFile);
                }
                timeToBreak++;
            }
        }

        return 0;
    }

    public void forwardDelete(String msg){
        String[] message = msg.split("#");
        String sucPortNo = message[0];

        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sucPortNo) * 2);
            OutputStream newStream = socket.getOutputStream();
            DataOutputStream newS = new DataOutputStream(newStream);
            newS.writeUTF(msg);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.get("key").toString();
        String value = values.get("value").toString();

        FileOutputStream outputStream;

        try {

            if ((pred == null) && (suc == null)) {
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            }

            else if (pred != null && suc != null) {
                String keyHash = genHash(key);

                if (keyHash.compareTo(pred) < 0 && keyHash.compareTo(hashPort) <= 0 && pred.compareTo(hashPort)>0) {
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                }

                else if (keyHash.compareTo(pred) > 0 && keyHash.compareTo(hashPort) > 0 && pred.compareTo(hashPort) > 0) {
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                }

                else if (keyHash.compareTo(pred) > 0 && keyHash.compareTo(hashPort) < 0) {
                    outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                    outputStream.write(value.getBytes());
                    outputStream.close();
                }
                else {
                    String forwardInsert = sucPortNo+"#"+"ForwardFileInsert"+"#"+key+"#"+value;
                    forwardInsert(forwardInsert);
                }
            }
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }
        return uri;
    }

    public void forwardInsert(String msg){
        String[] message = msg.split("#");
        String sucPort = message[0];

        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sucPort) * 2);
            OutputStream newStream = socket.getOutputStream();
            DataOutputStream newS = new DataOutputStream(newStream);
            newS.writeUTF(msg);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Context context = getContext();

        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr)) * 2);

        try {
            hashPort = genHash(portStr);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        //ServerSocket serverSocket = null;
        try {
            ServerSocket serverSocket = new ServerSocket(ServerPort);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
            // Log.e(TAG, "Not able to start server task");
        }


        String msg = portStr + "#" +"Join";

        if (Integer.parseInt(portStr) != 5554) {
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        }

        else if (Integer.parseInt(portStr) == 5554) {
            sortedHashMap.put(hashPort, portStr);
            //Log.i(TAG,"5554 entered into TreeMap");
        }


        return false;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        MatrixCursor cursor = null;
        //Log.d(TAG, "QueryMethod Selection: " + selection);

        if(selectionArgs!=null){
            if (selection.equals("*") && selectionArgs[1].equals("FileQuery")) {
                //FileInputStream inputStream = null;
                String[] dir = getContext().fileList();
                String returnPort = selectionArgs[0];

                Log.d(TAG, "Query:Length 14 : " + dir.length);
                //HashMap<String,String> KeyvalueMap =  new HashMap<String, String>();
                ArrayList<String >keyList = new ArrayList<String>();
                ArrayList<String>valueList = new ArrayList<String>();

                for (String s : dir) {
                    Log.d(TAG, "Inside loop query 14 : " + s);

                    try {
                        Log.i("Files 14 :", s);
                        FileInputStream fileInputStream = getContext().openFileInput(s);
                        InputStreamReader input = new InputStreamReader(fileInputStream);
                        BufferedReader br = new BufferedReader(input);
                        StringBuilder sb = new StringBuilder();
                        String Line = br.readLine();

                        //Reference - http://stackoverflow.com/questions/22264711/matrixcursor-with-non-db-content-provider

                        //cursor.addRow(new String[]{s, Line});

                        keyList.add(s.trim());
                        valueList.add(Line.trim());

                        Log.i("Key 14 :", s);
                        Log.i("value 14 :", Line);

                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                String keyString="";
                String valueString="";
                for (String s:keyList){
                    keyString += s+",";
                }
                Log.i(TAG,"KeyString :"+keyString);
                for (String s:valueList){
                    valueString +=s+",";
                }
                Log.i(TAG,"valueString :"+valueString);


                if (sucPortNo.equals(returnPort)) {
                    String returnMessage = returnPort+"#"+"returnAllToMain"+"#"+keyString+"#"+valueString+"#"+"true";
                    Log.i(TAG,"Return Message to 14 :" + returnPort);
                    returnedQuery(returnMessage);
                }else if (!sucPortNo.equals(returnPort)){
                    String fileQuery = sucPortNo+"#"+"FileQuery"+"#"+returnPort;
                    Log.i(TAG, "Forward Message Again to successor14 : " + sucPortNo);
                    forwardQuery(fileQuery);

                    String returnMessage = returnPort+"#"+"returnAllToMain"+"#"+keyString+"#"+valueString+"#"+"false";
                    returnedQuery(returnMessage);
                }
            }
            else if ((!selection.equals("*")) && (!selection.equals("@")) && (selectionArgs[1].equals("ForwardQuery"))) {
                //String[] sel = selection.split("#");
                //Log.d(TAG,"Selection: " + selection);
                String key = selection;
                String returnPort = selectionArgs[0];

                Log.i(TAG, "Key:" + key);
                Log.i(TAG, "ReturnPortNo:" + returnPort);


                try {
                    String keyHash = genHash(key);

                    if ((keyHash.compareTo(pred) < 0 && keyHash.compareTo(hashPort) <= 0 && pred.compareTo(hashPort) > 0)||(keyHash.compareTo(pred) > 0 && keyHash.compareTo(hashPort) > 0 && pred.compareTo(hashPort) > 0)||(keyHash.compareTo(pred) > 0 && keyHash.compareTo(hashPort) < 0)) {
                        try {
                            FileInputStream inputStream = getContext().openFileInput(key);
                            InputStreamReader input = new InputStreamReader(inputStream);
                            BufferedReader br = new BufferedReader(input);
                            StringBuilder sb = new StringBuilder();
                            String Line = br.readLine();

                            String returnMsg = returnPort+"#"+"returnToMain"+"#"+key+"#"+Line;

                            returnedQuery(returnMsg);

                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    else {

                        String forwardMsg = sucPortNo+"#"+"ForwardQuery"+"#"+returnPort+"#"+key;

                        forwardQuery(forwardMsg);
                        Log.i(TAG, "Key not found- forwarded to succ" + sucPortNo);
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }

        }
        else if (selectionArgs == null){

            if ((!selection.equals("*")) && (!selection.equals("@"))) {

                if (pred == null) {
                    try {
                        FileInputStream inputStream = getContext().openFileInput(selection);
                        InputStreamReader input = new InputStreamReader(inputStream);
                        BufferedReader br = new BufferedReader(input);
                        StringBuilder sb = new StringBuilder();
                        String Line = br.readLine();

                        //Reference - http://stackoverflow.com/questions/22264711/matrixcursor-with-non-db-content-provider
                        cursor = new MatrixCursor(new String[]{"key", "value"});
                        MatrixCursor.RowBuilder builder = cursor.newRow();
                        builder.add(selection);
                        builder.add(Line);

                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else if (pred != null) {
                    String key = selection;
                    try {
                        String keyHash = genHash(selection);
                        if ((keyHash.compareTo(pred) < 0 && keyHash.compareTo(hashPort) <= 0 && pred.compareTo(hashPort) > 0)||(keyHash.compareTo(pred) > 0 && keyHash.compareTo(hashPort) > 0 && pred.compareTo(hashPort) > 0)|| (keyHash.compareTo(pred) > 0 && keyHash.compareTo(hashPort) < 0)){
                            try {
                                FileInputStream inputStream = getContext().openFileInput(key);
                                InputStreamReader input = new InputStreamReader(inputStream);
                                BufferedReader br = new BufferedReader(input);
                                StringBuilder sb = new StringBuilder();
                                String Line = br.readLine();

                                //Reference - http://stackoverflow.com/questions/22264711/matrixcursor-with-non-db-content-provider
                                cursor = new MatrixCursor(new String[]{"key", "value"});
                                MatrixCursor.RowBuilder builder = cursor.newRow();
                                builder.add(key);
                                builder.add(Line);

                            } catch (FileNotFoundException e) {
                                e.printStackTrace();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            return cursor;
                        }  else {


                            String fileQuery = sucPortNo+"#"+"ForwardQuery"+"#"+portStr+"#"+key;
                            waitFlag = true;
                            //new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, fileQuery);
                            forwardQuery(fileQuery);
                            while (waitFlag = true){
                                if (returnKey!=null && returnKey.equals(selection) && returnValue!=null){
                                    cursor = new MatrixCursor(new String[]{"key","value"});
                                    MatrixCursor.RowBuilder builder = cursor.newRow();
                                    builder.add(returnKey);
                                    builder.add(returnValue);
                                    waitFlag = false;
                                    return cursor;
                                }
                            }
                            //Log.i(TAG, "Key not found- forwarded to succ" + sucPortNo);
                            //Log.i(TAG,"Key  :  value" +returnKey+"  :  "+returnValue);

                        }

                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }
            else if (selection.equals("@")){

                cursor = new MatrixCursor(new String[]{"key", "value"});
                FileInputStream inputStream = null;
                String[] dir = getContext().fileList();

                //Log.d(TAG,"Query:Length " + dir.length);

                for (String s: dir){
                    Log.d(TAG, "Inside loop query: " + s);

                    try {
                        Log.i("Files", s);
                        FileInputStream fileInputStream = getContext().openFileInput(s);
                        InputStreamReader input = new InputStreamReader(fileInputStream);
                        BufferedReader br = new BufferedReader(input);
                        StringBuilder sb = new StringBuilder();
                        String Line = br.readLine();

                        //Reference - http://stackoverflow.com/questions/22264711/matrixcursor-with-non-db-content-provider

                        cursor.addRow(new String[]{s, Line});

                        Log.i("Key", s);
                        Log.i("value", Line);

                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                cursor.moveToFirst();
                Log.e(TAG, "query: " + cursor.getCount());
                return cursor;

            }
            else if (selection.equals("*")){
                if (pred==null || suc==null) {

                    cursor = new MatrixCursor(new String[]{"key", "value"});
                    FileInputStream inputStream = null;
                    String[] dir = getContext().fileList();

                    //Log.d(TAG, "Query:Length " + dir.length);

                    for (String s : dir) {
                        Log.d(TAG, "Inside loop query11: " + s);

                        try {
                            Log.i("Files", s);
                            FileInputStream fileInputStream = getContext().openFileInput(s);
                            InputStreamReader input = new InputStreamReader(fileInputStream);
                            BufferedReader br = new BufferedReader(input);
                            StringBuilder sb = new StringBuilder();
                            String Line = br.readLine();

                            //Reference - http://stackoverflow.com/questions/22264711/matrixcursor-with-non-db-content-provider

                            cursor.addRow(new String[]{s, Line});

                            //Log.i("Key", s);
                            //Log.i("value", Line);

                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    cursor.moveToFirst();
                    //Log.e(TAG, "query: " + cursor.getCount());
                    return cursor;
                }
                else if (pred!=null || suc!= null){


                    cursor = new MatrixCursor(new String[]{"key", "value"});
                    FileInputStream inputStream = null;
                    String[] dir = getContext().fileList();
                   // HashMap<String , String > tempValueStorage = new HashMap<String, String>();
                    ArrayList<String>KeyList = new ArrayList<String>();
                   ArrayList<String >ValueList = new ArrayList<String>();

                    Log.d(TAG, "Query:Length12 : " + dir.length);

                    for (String s : dir) {
                        Log.d(TAG, "Inside loop query12: " + s);

                        try {
                            Log.i("Files12 : ", s);
                            FileInputStream fileInputStream = getContext().openFileInput(s);
                            InputStreamReader input = new InputStreamReader(fileInputStream);
                            BufferedReader br = new BufferedReader(input);
                            StringBuilder sb = new StringBuilder();
                            String Line = br.readLine();

                            //Reference - http://stackoverflow.com/questions/22264711/matrixcursor-with-non-db-content-provider
                   //         tempValueStorage.put(s,Line);
                           KeyList.add(s);
                           ValueList.add(Line);

                            //cursor.addRow(new String[]{s, Line});

                            Log.i("Key12 : ", s);
                            Log.i("value12 :", Line);

                        } catch (FileNotFoundException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }


                    String fileQuery = sucPortNo+"#"+"FileQuery"+"#"+portStr;
                    forwardQuery(fileQuery);

                    waitFlag =true;

                    //FinalValsList.addAll(KeyList);
                    //FinalValsList.addAll(ValueList);
                    Log.i(TAG,"waitFlag :"+waitFlag);

                    while (waitFlag==true){
                        try {
                            if (returnType.equals("returnAllToMain") && CheckFlag.equals("true") && FinalKeysList.size() != 0) {

                                waitFlag = false;
                            }
                        }catch (NullPointerException e) {
                            e.printStackTrace();
                        }
                    }

                    FinalKeysList.addAll(KeyList);
                    FinalValsList.addAll(ValueList);

                    Log.i(TAG, "Size of Final Key Array List :" + FinalKeysList.size());
                    Log.i(TAG, "Size of Final Value Array List" + FinalValsList.size());

                    for (int i=0; i<FinalKeysList.size();i++){
                        cursor.addRow(new String[]{FinalKeysList.get(i), FinalValsList.get(i)});
                        //Log.i("Key13 :", FinalKeysList.get(i));
                        //Log.i("value13 :", FinalValsList.get(i));
                    }

                    cursor.moveToFirst();
                    Log.e(TAG, "queryFinal12 : " + cursor.getCount());
                    return cursor;
                }
            }

            Log.d(TAG, "query: here");
            return cursor;
        }

        return cursor;

    }

    public void forwardQuery(String msg){
        String[] message = msg.split("#");
        String sucPort = message[0];
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(sucPort) * 2);
            OutputStream newStream = socket.getOutputStream();
            DataOutputStream newS = new DataOutputStream(newStream);
            newS.writeUTF(msg);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void returnedQuery(String msg){
        String[] message = msg.split("#");
        String retPort = message[0];
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(retPort) * 2);
            OutputStream newStream = socket.getOutputStream();
            DataOutputStream newS = new DataOutputStream(newStream);
            newS.writeUTF(msg);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            //String msg;

            try {
                while (true) {
                    Socket mySocket = serverSocket.accept();
                    DataInputStream newStream = new DataInputStream(mySocket.getInputStream());

                    String message = newStream.readUTF();

                    String[] msg = message.split("#");

                    String msgType = msg[1];
                    String msgPort = msg[0];


                    if (msgType.equals("Join")) {


                        Log.i(TAG, "join request received");

                        String hashIncPort = genHash(msgPort);
                        sortedHashMap.put(hashIncPort, msgPort);

                        pred = hashIncPort;
                        suc = hashIncPort;

                        if ((pred!=null)&&(suc!=null)){

                        ArrayList<String> hashList = new ArrayList<String>(sortedHashMap.keySet());
                        ArrayList<String> portList = new ArrayList<String>(sortedHashMap.values());

                        //Update all avd's of new predecessors and successors...

                            for (int j = 0; j < portList.size(); j++) {

                                String hashing = genHash(portList.get(j));

                                for (int i = 0; i < hashList.size(); i++) {

                                    if ((hashList.get(i).equals(hashing)) && (i == 0)) {
                                        suc = hashList.get(i + 1);
                                        pred = hashList.get(hashList.size() - 1);
                                    } else if ((hashList.get(i).equals(hashing)) && (i == (hashList.size() - 1))) {
                                        suc = hashList.get(0);
                                        pred = hashList.get(i - 1);
                                    } else if (((hashList.get(i).equals(hashing)) && (i != 0) && (i != (hashList.size() - 1)))) {
                                        suc = hashList.get(i + 1);
                                        pred = hashList.get(i - 1);
                                    }
                                }

                                predPortNo = sortedHashMap.get(pred);
                                sucPortNo = sortedHashMap.get(suc);
                                chordsize = String.valueOf(sortedHashMap.size());

                                String updateMessage = portList.get(j) + "#" + "updatePredSuc" + "#" + predPortNo + "#" + sucPortNo + "#" + pred + "#" + suc + "#" + chordsize;


                                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, updateMessage);

                            }
                        }


                } else if (msgType.equals("updatePredSuc")) {
                        pred = msg[4];
                        suc = msg[5];
                        predPortNo = msg[2];
                        sucPortNo = msg[3];
                        chordsize = msg[6];

                        Log.i(TAG,"Pred : Node : Suc"+predPortNo+" : "+portStr+" : "+sucPortNo);
                        Log.i(TAG,"Chord Size :"+chordsize);


                } else if (msgType.equals("ForwardFileInsert")) {
                        ContentValues cv = new ContentValues();
                        String key = msg[2];
                        String value = msg[3];
                        cv.put("key", key);
                        cv.put("value", value);
                        insert(Content_URI, cv);
                    }
                    else if (msgType.equals("forwardDelete")) {
                        String selection = msg[2];
                        delete(Content_URI, selection, null);
                    }
                    else if (msgType.equals("FileQuery")) {
                        String selection = "*";
                        String returnPort = msg[2];
                        String[] selectionArgs = {returnPort, "FileQuery"};
                        query(Content_URI, null, selection, selectionArgs, null);
                    }
                    else if (msgType.equals("ForwardQuery")) {
                        String selection = msg[3];
                        String returnPort = msg[2];
                        String[] selectionArgs = {returnPort, "ForwardQuery"};
                        query(Content_URI, null, selection, selectionArgs, null);

                    }
                    else if (msgType.equals("returnToMain")) {

                        returnKey = msg[2];
                        returnValue=msg[3];
                        returnType = "returnToMain";
                        waitFlag= false;
                    }
                    else if (msgType.equals("returnAllToMain")){

                        //HashMap<String ,String > temp= new HashMap<String, String>();

                        //String flag = msg[4];
                        String keyList = msg[2];
                        String valueList =msg[3];
                        returnType = "returnAllToMain";

                        //Convert the string into arrayList
                        ArrayList<String > Keys = new ArrayList<String>(Arrays.asList(keyList.split(",")));
                        ArrayList<String > Vals = new ArrayList<String>(Arrays.asList(valueList.split(",")));

                        Log.i(TAG, "Keys recieved :" + Keys.size());
                        Log.i(TAG, "values recieved :" + Vals.size());


                        //Combine the intermediate arrayList with Main ArrayList.
                        KeysListMain.addAll(Keys);
                        ValuesListMain.addAll(Vals);
                        Log.i(TAG, "Files Appended : " + KeysListMain.size());
                        Log.i(TAG,"Values Appended :"+ ValuesListMain.size());

                        count++;

                        if (chordsize.equals(String.valueOf(count))){
                            FinalKeysList.addAll(KeysListMain);
                            FinalValsList.addAll(ValuesListMain);
                            Log.i(TAG, "All nodes checked, files added to Final List" + FinalKeysList.size());
                            CheckFlag ="true";
                            Log.i(TAG,"Value of Check Flag :"+ CheckFlag);
                        }



/*
*//*
                        KeysListMain.addAll(Keys);
                        ValuesListMain.addAll(Vals);*//*

                        temp1.put();

                        for (int i=0; i<Keys.size();i++){
                            temp1.put(Keys.get(i),Vals.get(i));
                        }


                        //temp.putAll(msg.getAllFilesStored());
                        Log.i(TAG, "Size of Key/Value Lists recieved 15 : "+Keys.size());

                        returnType = "returnAllToMain";
                        //temp1.putAll(temp);
                        count++;
                        Log.i(TAG,"Size of temp1 : "+temp1.size());
                        Log.i(TAG,"Count of AVD's that have returned the value15 :"+ count);

                        //Log.i(TAG,"Old Size of Node HashMap :"+allFilesStored.size());
                        //Log.i(TAG, "Size of received HashMap :" + temp.size());
                        //allFilesStored.putAll(temp);

                        //Log.i(TAG,"New Size of Node HashMao :"+allFilesStored.size());

                        if (flag.equals("true") && chordsize.equals(String.valueOf(count))){
                            allFilesStored.putAll(temp1);

                            Log.i(TAG,"Size of HashMap to be added :"+allFilesStored.size());

                        }else if (flag.equals("false")){
                            waitFlag=true;
                        }*/
                    }


            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }
}

public class ClientTask extends AsyncTask<String , Void, Void> {

    @Override
    protected Void doInBackground(String ... params) {


        String message = params[0];
        String[] msg = message.split("#");

        String msgType = msg[1];
        String msgPort = msg[0];
        Log.i(TAG,"port"+msgPort);
        Log.i(TAG,"Type"+msgType);

        if (msgType.equals("Join")) {

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11108);
                OutputStream newStream = socket.getOutputStream();
                DataOutputStream newS = new DataOutputStream(newStream);
                newS.writeUTF(message);
                socket.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
            Log.i(TAG, "Join Request sent to 5554");
        }

        else if (msgType.equals("updatePredSuc")) {

            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgPort) * 2);
                OutputStream newStream = socket.getOutputStream();
                DataOutputStream newS = new DataOutputStream(newStream);
                newS.writeUTF(message);
                socket.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
            //Log.i(TAG, "Update Predecessor request sent by 5554 to other avds");
        }

        return null;
    }
}

}
