package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.NetworkOnMainThreadException;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();

	private static String myPort, myNodeId;

	static final String[] REMOTE_PORTS = {"11108", "11112", "11116", "11120", "11124"};
	static final String[] EMU_PORTS = {"5554", "5556", "5558", "5560", "5562"};
	static final int SERVER_PORT = 10000;

	public static TreeMap<String, String> chordRing;
	public HashMap<String, String> providerData = new HashMap<String, String>();
	public HashMap<String, String> keyCursorHash = new HashMap<String, String>();
	public HashMap<String, String> allCursorHash = new HashMap<String, String>();
	public HashMap<String, String> getDataCursorHash = new HashMap<String, String>();


	int count_insert = 0, count_insert_repl = 0;
	int count_query_resp_all = 0;
	int count_get_data_resp_all = 0;
	int born = 0;

    static String[] REPL_PORTS = {null, null};
    static String[] PRED_PORTS = {null, null};


	static final String KEY = "key";
	static final String VALUE = "value";
//	static final UriMatcher uriMatcher;
//	static{
//		uriMatcher = new UriMatcher(UriMatcher.NO_MATCH);
//	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("*")){
			providerData.clear();
			new JoinClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.DELETE,
					Constants.BLANK, "*");
		}else if(!selection.equals("@")){
			providerData.remove(selection);
			new JoinClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.DELETE,
					Constants.BLANK, selection);
		}else if(selection.equals("@")){
			try {


				if (chordRing.size() == 1) {
					for (String item : providerData.keySet()) {
						providerData.remove(item);
					}
					return 0;
				}

				if (!myPort.equals(chordRing.firstKey())) {
					printRing("From @ delete method" + myPort);

					String myPredNodeId = genHash(chordRing.lowerKey(myPort));

					for (String item : providerData.keySet()) {
						String itemHash = genHash(item);

						if (myPredNodeId.compareTo(itemHash) < 0 && myNodeId.compareTo(itemHash) >= 0) {
							providerData.remove(item);
						}
					}

				} else {
					String lastNodeId = genHash(chordRing.lastKey());
					int count = 0;
					for (String item : providerData.keySet()) {
						String itemHash = genHash(item);
						Log.i(TAG, lastNodeId + " - " + itemHash + " - " + myNodeId);
						if (lastNodeId.compareTo(itemHash) >= 0 && myNodeId.compareTo(itemHash) < 0) {

						}else {
							Log.i(TAG, "Count "+Integer.toString(count++));
							providerData.remove(item);
						}
					}
				}
			}catch (NoSuchAlgorithmException e){
				e.printStackTrace();
			}
			new JoinClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.DELETE,
					Constants.BLANK, selection);

		}
		return 0;
	}

	private Void handleDelete(SimpleDhtMessage message){
		if(message.msgData.equals("*")){
			providerData.clear();

		}else if(!message.msgData.equals("@")){
			providerData.remove(message.msgData);
		}else if(message.msgData.equals("@")){
			String myPredNodeId = chordRing.lowerKey(myPort);

			if(!myPort.equals(chordRing.firstKey())) {
				for (String item : providerData.keySet()) {
					String itemHash = providerData.get(item);
					if (myPredNodeId.compareTo(itemHash) > 0 && itemHash.compareTo(myNodeId) < 0) {
						providerData.remove(item);
					}
				}
			}else {
				for (String item : providerData.keySet()) {
					String itemHash = providerData.get(item);
					if (myPredNodeId.compareTo(itemHash) > 0 && itemHash.compareTo(myNodeId) > 0) {
						providerData.remove(item);
					}
				}
			}
		}
		return null;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

        printRing("From Insert ");
        String key = values.getAsString(KEY);
        String value = values.getAsString(VALUE);

        String ownerNode = getOwnerNode(key);
        Log.i(TAG, spaceString("OwnerNode", ownerNode));
        String msgType = null;
        String port = null;
        String msgData = null;

        if(myPort.equals(ownerNode)){
            // If belongs to me
            storeMessage(Constants.INSERT, key, value);
            Log.i(TAG, spaceString("Inserted on me", myPort, key, value));
            // Replicate
            msgType = Constants.INSERT_REPL;
            port = myPort;
            msgData = key + Constants.DATA_PIPE + value;
        }else{
            // Propagate
            Log.i(TAG, spaceString("Propagating to ", ownerNode, key, value));
            msgType = Constants.INSERT_PROP;
            port = ownerNode;
            msgData = key + Constants.DATA_PIPE + value;
        }

        new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgType,
                port, msgData
        );



        Log.v("insert ", values.toString());
		return null;

	}


    public Void handleInsert(SimpleDhtMessage message) {
        String[] pair = message.msgData.split(Constants.DATA_PIPE);
        String key = pair[0], value = pair[1];

        if(message.msgType.equals(Constants.INSERT_REPL_REQ)){
            // If replication request
            storeMessage(Constants.INSERT_REPL_REQ, key, value);
            Log.i(TAG, spaceString("Insert Replicated on me from", message.srcPort, key, value));

        }else if(message.msgType.equals(Constants.INSERT_REQ)){
            // Propagated Insert
            storeMessage(Constants.INSERT_REQ, key, value);
            Log.i(TAG, spaceString(message.msgType, myPort, key, value));
            Log.i(TAG, spaceString("Insert propagated to me from", message.srcPort));
            String msgType = Constants.INSERT_REPL;
            String port = myPort;

            new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgType,
                    port, key + Constants.DATA_PIPE + value
            );
        }
        return null;
    }


	public String getOwnerNode(String key)  {

        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        for(Map.Entry<String, String> entry: chordRing.entrySet()){

            if (chordRing.firstKey().equals(entry.getKey())){
                String lastNodeId = chordRing.lastEntry().getValue();
                if(lastNodeId.compareTo(keyHash) < 0 || keyHash.compareTo(entry.getValue()) <= 0){
                    return entry.getKey();
                }

            }else{
                String prevNodeId = chordRing.lowerEntry(entry.getKey()).getValue();
                if(prevNodeId.compareTo(keyHash) < 0 && keyHash.compareTo(entry.getValue()) <= 0){
                    return entry.getKey();
                }
            }
        }
	    return null;
    }

    public String[] getReplPorts(){

        Log.i(TAG, "In GetReplPorts() ");

        if(myPort.equals(chordRing.lastKey())){
            REPL_PORTS[0] = chordRing.firstKey();
        }else {
            REPL_PORTS[0] = chordRing.higherKey(myPort);
        }

        if(REPL_PORTS[0].equals(chordRing.lastKey())){
            REPL_PORTS[1] = chordRing.firstKey();
        }else {
            REPL_PORTS[1] = chordRing.higherKey(REPL_PORTS[0]);
        }

        Log.i(TAG, spaceString("Repl ports ", REPL_PORTS[0], REPL_PORTS[1]));

	    return REPL_PORTS;
    }

    public String[] getPredPorts(){

        Log.i(TAG, "In GetPredPorts() ");

        if(myPort.equals(chordRing.firstKey())){
            PRED_PORTS[0] = chordRing.lastKey();
        }else {
            PRED_PORTS[0] = chordRing.lowerKey(myPort);
        }

        if(PRED_PORTS[0].equals(chordRing.firstKey())){
            PRED_PORTS[1] = chordRing.lastKey();
        }else {
            PRED_PORTS[1] = chordRing.lowerKey(PRED_PORTS[0]);
        }

        Log.i(TAG, spaceString("Pred ports ", PRED_PORTS[0], PRED_PORTS[1]));

        return PRED_PORTS;
    }



    public Void storeMessage(String msgType, String key, String value){
        providerData.put(key, value);

        if(msgType.equals(Constants.INSERT)){
            count_insert++;
            Log.i(TAG, msgType +"@"+ myPort + " " + count_insert + " " + key + " " + value);

        }else if(msgType.equals(Constants.INSERT_REPL)){
            count_insert_repl++;
            Log.i(TAG, msgType+ "@"+ myPort + " " + count_insert_repl + " " + key + " " + value);
        }
	    return null;
    }

    private void createChordRing(){

	    for(String item: EMU_PORTS){
            try {
                chordRing.put(item, genHash(item));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }
    }

    private Void handleJoin(SimpleDhtMessage message){

        if(message.msgType.equals(Constants.JOIN_REQ)){
            Log.i(TAG, "Node_Join req " + message.srcPort);
            try {
                chordRing.put(message.srcPort, genHash(message.srcPort));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            update_source_node(message.srcPort);


        }else if(message.msgType.equals(Constants.JOIN_UPDATE)){
//            Log.i(TAG, "Node update request recieve by " + myPort+ " list"+ message.msgData);

            if(message.msgData.contains(Constants.DATA_PIPE)) {
//                Log.i(TAG, "Msg Data "+message.msgData);
                String[] list = message.msgData.split(Constants.DATA_PIPE);
                for (int i = 0; i < list.length; i++){
//                    Log.i(TAG, "Split Data "+list[i]);
                    try {
                        chordRing.put(list[i], genHash(list[i]));
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
            }else{
                try {
                    chordRing.put(message.msgData, genHash(message.msgData));
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
//            Log.i(TAG, "Ring " + chordRing.keySet().toString());
        }
        return null;
    }

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

        SharedPreferences prefs = getContext().getSharedPreferences("PermanentData",
                Context.MODE_PRIVATE);
        PermanentData permanentData = new PermanentData(prefs);

        chordRing = new TreeMap<String, String>(new Comparator<String>() {
            @Override
            public int compare(String lhs, String rhs) {
                try{
                    lhs = genHash(lhs);
                    rhs = genHash(rhs);
                }catch(NoSuchAlgorithmException e){
                    Log.i(TAG, e.toString());
                }
                return lhs.compareTo(rhs);
            }
        });

//        createChordRing();

        TelephonyManager tel = (TelephonyManager) this.getContext().
                getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = portStr;

        portStr = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.i(TAG, "Port Number : " + myPort);


        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NetworkOnMainThreadException e) {
            e.printStackTrace();
        }

        try {
            myNodeId = genHash(myPort);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (!portStr.equals(REMOTE_PORTS[0])){
            Log.e(TAG, "Client Task being created");
            new JoinClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.JOIN_REQ,
                    myPort, Constants.BLANK);
        }else {

            chordRing.put(myPort, myNodeId);
        }

        if(permanentData.get() > 1) {
            Log.i(TAG, spaceString("Permanent Data Value", ""+permanentData.get()));

            permanentData.put(permanentData.get()+1);
            new GetDataClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                    Constants.SEND_DATA_REQ,
                    Constants.EMPTY,
                    Constants.EMPTY);
        }
        return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor cursor = new MatrixCursor(
                new String[] {KEY, VALUE}
        );

        if(selection.equals("@")){
            Log.i(TAG, "Query @");

            for(String item: providerData.keySet()) {
                cursor.addRow(new String[]{item, providerData.get(item)});
            }

        }else if(selection.equals("*")){
            for(String item: providerData.keySet()) {
                allCursorHash.putAll(providerData);
            }

            String msgType = Constants.QUERY_REQ_ALL;
            String port = myPort;
            String msgData = Constants.EMPTY;

            new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgType, port, msgData);

            try {
                synchronized (allCursorHash) {
                    allCursorHash.wait();
                }
            }catch (InterruptedException e){
                e.printStackTrace();
            }

            for(int i = 0; i < 100; ++i)
            for(Map.Entry<String, String> entry : allCursorHash.entrySet()) {
                cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
            }

        }else{
            if(providerData.containsKey(selection)){
                Log.i(TAG, spaceString("Query key found on me", selection,
                        providerData.get(selection)));
                cursor.addRow(new String[]{selection, providerData.get(selection)});
                return cursor;
            }
            String ownerNode = getOwnerNode(selection);

            Log.i(TAG, spaceString("Query key propagating to ", ownerNode, selection));
            String msgType = Constants.QUERY_REQ_KEY;
            String port = ownerNode;
            String msgData = selection;

            new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgType, port, msgData);

            try {
                synchronized (keyCursorHash) {
                    keyCursorHash.wait();
                }
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            cursor.addRow(new String[]{selection, keyCursorHash.get(selection)});

        }

        return cursor;

	}

	// Server Side Queries
	public Void handleQuery(SimpleDhtMessage message){
	    // When request arrives at destination
        Log.i(TAG, spaceString(message.toString()));
	    if(message.msgType.contains(Constants.QUERY_REQ)){
            String msgType = null;
            String port = null;
            String msgData = null;
	        if(message.msgType.equals(Constants.QUERY_REQ_KEY)) {
	            String key = message.msgData;
                if (providerData.containsKey(key)) {
                    msgType = Constants.QUERY_RESP_KEY;
                    port = message.srcPort;
                    msgData = key + Constants.DATA_PIPE + providerData.get(key);
                }
                Log.i(TAG, spaceString("Query key responding back to", port, msgData));
            }else if(message.msgType.equals(Constants.QUERY_REQ_ALL)){
//                String key = message.msgData;

                msgData = "";
                msgType = Constants.QUERY_RESP_ALL;
                port = message.srcPort;
                for (Map.Entry<String, String> pair: providerData.entrySet()) {
                    msgData += pair.getKey() + Constants.DATA_PIPE + pair.getValue() +
                            Constants.KVDELIMETER;
                }
                Log.i(TAG, spaceString("Query All responding back to", port, msgData));
            }
            // Response back to source
            new QueryClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgType, port, msgData);

        }// When response arrives at the source
        else if(message.msgType.contains(Constants.QUERY_RESP)){
            if(message.msgType.equals(Constants.QUERY_RESP_KEY)){
                String[] pair = message.msgData.split(Constants.DATA_PIPE);
                String key = pair[0], value = pair[1];

                synchronized (keyCursorHash){
                    keyCursorHash.put(key, value);
                    keyCursorHash.notify();
                }

                Log.i(TAG, spaceString("Query key response received from", message.srcPort,
                        key, value));

            }else if(message.msgType.equals(Constants.QUERY_RESP_ALL)){
                String[] wholeData = message.msgData.split(Constants.KVDELIMETER);

                synchronized (allCursorHash) {

                    for (String oneRow : wholeData) {
                        String[] pair = oneRow.split(Constants.DATA_PIPE);
                        String key = pair[0], value = pair[1];
                        allCursorHash.put(key, value);
                    }
                    count_query_resp_all++;
                    if(count_query_resp_all >=3) {
                        allCursorHash.notify();
                    }
                }
                Log.i(TAG, spaceString("Query All response received from", message.srcPort,
                        message.msgData));
            }
            // Response back to source
        }
	    return null;
    }

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
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

    private Void update_source_node(String srcPort) {

//        if(!chordRing.isEmpty()) {
        String data = new String();
        for (String item : chordRing.keySet()) {
//            Log.i(TAG, "Data item " + item);
            data += item + Constants.DATA_PIPE;
        }
//        Log.i(TAG, "Data Len " + data.length());
        data = data.substring(0, data.length() - 2);
//        Log.i(TAG, "Send to source to update list "+srcPort+" Node list"+data);
        new JoinClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.JOIN_UPDATE, srcPort,
                data);
//    }
        return null;
    }

    private Void handleSend(SimpleDhtMessage message){


        if(message.msgType.equals(Constants.SEND_DATA_REQ)) {
            String msgType = message.msgType;
            String port = message.srcPort;
            String msgData = "";
            String[] reqPorts = message.msgData.split(Constants.DATA_PIPE);
            for(Map.Entry<String, String> entry: providerData.entrySet()) {
                String ownerNode = getOwnerNode(entry.getKey());
                if (ownerNode.equals(reqPorts[0])) {
                    msgData += entry.getKey() + Constants.DATA_PIPE + entry.getValue() +
                            Constants.KVDELIMETER;

                }else if(reqPorts.length == 2 && ownerNode.equals(reqPorts[1])){

                    msgData += entry.getKey() + Constants.DATA_PIPE + entry.getValue() +
                            Constants.KVDELIMETER;
                }
            }
            Log.i(TAG, spaceString("Sending Request data back to", port, msgData));

            new GetDataClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgType, port, msgData);

        }else if(message.msgType.equals(Constants.SEND_DATA_RESP)){
            String[] wholeData = message.msgData.split(Constants.KVDELIMETER);

            synchronized (getDataCursorHash) {

                for (String oneRow : wholeData) {
                    String[] pair = oneRow.split(Constants.DATA_PIPE);
                    String key = pair[0], value = pair[1];
                    getDataCursorHash.put(key, value);
                }
                count_get_data_resp_all++;
                if(count_get_data_resp_all >=3) {
                    getDataCursorHash.notify();
                }
            }
            Log.i(TAG, spaceString("Send data response received from", message.srcPort,
                    message.msgData));
        }

	    return null;
    }


    private Void decodeMessage(String[] msg){
        SimpleDhtMessage message = new SimpleDhtMessage(msg[0], msg[1], msg[2]);
        Log.i(TAG, spaceString("Decode Mesaage", message.toString()));
        if(message.msgType.contains("join")) {
            handleJoin(message);

        }else if(message.msgType.contains(Constants.INSERT)){
            handleInsert(message);

        }else if(message.msgType.contains((Constants.DELETE))){
            handleDelete(message);

        }else if(message.msgType.contains((Constants.QUERY))){
            handleQuery(message);
        }else if(message.msgType.contains(Constants.SEND)){
            handleSend(message);
        }

        return null;
    }

    private void printRing(String toPrint){
        Log.i(TAG, "Print Ring "+toPrint+chordRing.keySet().toString());
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private final String TAG = ServerTask.class.getSimpleName();

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */

            try {

                while (serverSocket != null){
                    Socket socket = serverSocket.accept();
                    BufferedReader bfrRdr = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    String inp = bfrRdr.readLine();
                    String[] msg = inp.split(Constants.MSG_SEP);

                    decodeMessage(msg);


                    bfrRdr.close();
                    socket.close();
                }

            }
            catch (IOException e) {
                e.printStackTrace();
            }


            return null;
        }

    }

    public String spaceString(String... items){
	    String msg = "";
	    for (String item: items){
	        msg += item + " : ";
        }
        return msg;
    }


    private class InsertClientTask extends AsyncTask<String, Void, Void> {

        private final String TAG = InsertClientTask.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {

//            SimpleDhtMessage message = new SimpleDhtMessage(msgs[0], msgs[1], msgs[2]);

            String[] destNodesList = null;
            String msgType =  null;
            String srcPort =  myPort;
            String msgData = null;
            if(msgs[0].equals(Constants.INSERT_REPL)) {
                Log.i(TAG, Constants.INSERT_REPL);
                msgType = Constants.INSERT_REPL_REQ;
                destNodesList = getReplPorts();
                msgData = msgs[2];

            }else if(msgs[0].equals(Constants.INSERT_PROP)){
                msgType = Constants.INSERT_REQ;
                destNodesList = new String[]{msgs[1]};
                msgData = msgs[2];
            }


            Socket socket;
            for(String destNode: destNodesList) {
                try {
                    Log.i(TAG, spaceString(msgType, "From", srcPort, "To", destNode, msgData));
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destNode) * 2);

                    BufferedWriter bfrWtr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bfrWtr.write(msgType + Constants.MSG_SEP + srcPort + Constants.MSG_SEP + msgData);
                    bfrWtr.flush();
                    socket.close();

                } catch (ConnectException e) {
                    continue;

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return null;
        }
    }

    private class JoinClientTask extends AsyncTask<String, Void, Void> {

        private final String TAG = JoinClientTask.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {

            Log.e(TAG, "SimpleMessage : "+msgs[0]+ msgs[1]+msgs[2]);
//            SimpleDhtMessage message = new SimpleDhtMessage(msgs[0], msgs[1], msgs[2]);

            Socket socket;
            for(int i = 0; i < 5; i++) {
                try {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(REMOTE_PORTS[i]));

                    BufferedWriter bfrWtr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bfrWtr.write(msgs[0] + Constants.MSG_SEP + msgs[1] + Constants.MSG_SEP + msgs[2]);
                    bfrWtr.flush();
                    socket.close();

                }catch (ConnectException e)
                {
                    continue;
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return null;
        }
    }

    private class QueryClientTask extends AsyncTask<String, Void, Void> {

        private final String TAG = QueryClientTask.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {

            String[] destNodesList = null;
            String msgType =  null;
            String srcPort =  myPort;
            String msgData = null;
            if(msgs[0].equals(Constants.QUERY_REQ_KEY)){
                destNodesList = new String[]{msgs[1]};
                msgType = msgs[0];
                msgData = msgs[2];
            }else if(msgs[0].equals(Constants.QUERY_RESP_KEY)){
                destNodesList = new String[]{msgs[1]};
                msgType = msgs[0];
                msgData = msgs[2];
            }else if(msgs[0].equals(Constants.QUERY_REQ_ALL)){
                destNodesList = new String[4];
                int i = 0;
                for(String nodeId: EMU_PORTS){
                    if(!nodeId.equals(myPort)){
                        destNodesList[i] = nodeId;
                        i++;
                    }
                }
                msgType = msgs[0];
                msgData = msgs[2];
            }else if(msgs[0].equals(Constants.QUERY_RESP_ALL)){
                destNodesList = new String[]{msgs[1]};
                msgType = msgs[0];
                msgData = msgs[2];
            }

            Socket socket;
            for(String destNode: destNodesList) {
                try {
                    Log.i(TAG, spaceString(msgType, "From", srcPort, "To", destNode, msgData));
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destNode) * 2);

                    BufferedWriter bfrWtr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bfrWtr.write(msgType + Constants.MSG_SEP + srcPort + Constants.MSG_SEP + msgData);
                    bfrWtr.flush();
                    socket.close();

                } catch (ConnectException e) {
                    continue;

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


            return null;
        }
    }

    private class GetDataClientTask extends AsyncTask<String, Void, Void> {

        private final String TAG = GetDataClientTask.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {

            SimpleDhtMessage[] simpleMsgs = new SimpleDhtMessage[4];
            getPredPorts();
            getReplPorts();

            if(msgs[0].equals(Constants.SEND_DATA_REQ)){

                // For nearest predecessor send two Port Ids
                for(int i = 0; i < 4; ++i){
                    simpleMsgs[i] = new SimpleDhtMessage();
                }
                simpleMsgs[0].msgType = Constants.SEND_DATA_REQ; simpleMsgs[0].srcPort = myPort;
                simpleMsgs[0].msgData = PRED_PORTS[0] + Constants.DATA_PIPE + PRED_PORTS[1];
                simpleMsgs[0].destPort = PRED_PORTS[0];

                simpleMsgs[1].msgType = Constants.SEND_DATA_REQ; simpleMsgs[1].srcPort = myPort;
                simpleMsgs[1].msgData = PRED_PORTS[1]; simpleMsgs[1].destPort = PRED_PORTS[1];

                simpleMsgs[2].msgType = Constants.SEND_DATA_REQ; simpleMsgs[2].srcPort = myPort;
                simpleMsgs[2].msgData = myPort; simpleMsgs[2].destPort = REPL_PORTS[0];

                simpleMsgs[3].msgType = Constants.SEND_DATA_REQ; simpleMsgs[3].srcPort = myPort;
                simpleMsgs[3].msgData = myPort; simpleMsgs[3].destPort = REPL_PORTS[1];


                Socket socket;
                for(SimpleDhtMessage message: simpleMsgs) {
                    try {
                        Log.i(TAG, spaceString(message.msgType, "From", message.srcPort, "To",
                                message.destPort, message.msgData));

                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(message.destPort) * 2);

                        BufferedWriter bfrWtr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                        bfrWtr.write(message.msgType + Constants.MSG_SEP + message.srcPort +
                                Constants.MSG_SEP + message.msgData);
                        bfrWtr.flush();
                        socket.close();

                    } catch (ConnectException e) {
                        continue;

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }else if(msgs[0].equals(Constants.SEND_DATA_RESP)){
                String destNode = msgs[1];
                String msgType =  msgs[0];
                String srcPort =  myPort;
                String msgData = msgs[2];

                Socket socket;

                try {
                    Log.i(TAG, spaceString(msgType, "From", srcPort, "To",
                            destNode, msgData));
                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(destNode) * 2);

                    BufferedWriter bfrWtr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bfrWtr.write(msgType + Constants.MSG_SEP + srcPort +
                            Constants.MSG_SEP + msgData);
                    bfrWtr.flush();
                    socket.close();
                } catch (ConnectException e) {
                    Log.e(TAG, "Connect Failure");

                } catch (IOException e) {
                    e.printStackTrace();
                }


            }




            return null;
        }
    }

}
