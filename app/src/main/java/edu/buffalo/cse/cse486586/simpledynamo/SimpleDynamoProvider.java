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
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
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

	public static TreeMap<String, String> chordRing = null;
	public HashMap<String, String> providerData = new HashMap<String, String>();

	int count_insert = 0, count_insert_repl = 0;


//	static final String KEY = "key";
//	static final String VALUE = "value";
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

        String key = values.getAsString("key");
        String value = values.getAsString("value");

        try {

            String keyHash = genHash(key);

            // If the current node is first node
            if(myPort.equals(chordRing.firstKey())){


                String tmpLastId = chordRing.get(chordRing.lastKey());
                // If key belongs to me
                if(tmpLastId.compareTo(keyHash) < 0 || keyHash.compareTo(myNodeId) <= 0 ){
//                    Log.i(TAG, "First Belong");
                    printRing("in First Belong");
                    storeMessage(Constants.INSERT, key, value);
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_REPL,
                            key, value);
                }else if(keyHash.compareTo(myNodeId) > 0){
                    // Propagate to next node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.higherKey(myPort))),
                            key, value);
                }
            }// If the current node is last node
            else if(myPort.equals(chordRing.lastKey())){
                String tmpPrevId = chordRing.get(chordRing.lowerKey(myPort));
                // If key belongs to me
                if(tmpPrevId.compareTo(keyHash) < 0 && keyHash.compareTo(myNodeId) <= 0 ){
                    storeMessage(Constants.INSERT+"Belong", key, value);
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_REPL,
                            key, value);
                }else if(keyHash.compareTo(myNodeId) > 0){
                    // Propagate to next(first) node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.firstKey())),
                            key, value);

                }else{
                    // Propagate to prev node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.lowerKey(myPort))),
                            key, value);
                }
            }// If the current node is middle node
            else{
                String tmpPrevId = chordRing.get(chordRing.lowerKey(myPort));
                // If key belongs to me
                if(tmpPrevId.compareTo(keyHash) < 0 && keyHash.compareTo(myNodeId) <= 0 ){
                    storeMessage(Constants.INSERT+"Belong", key, value);
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_REPL,
                            key, value);
                }else if(keyHash.compareTo(myNodeId) > 0){
                    // Propagate to next node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.higherKey(myPort))),
                            key, value);

                }else{
                    // Propagate to prev node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.lowerKey(myPort))),
                            key, value);
                }

            }

//            if (!myPort.equals(chordRing.firstKey())) {
//                String myPredNodeId = genHash(chordRing.lowerKey(myPort));
//                if (myPredNodeId.compareTo(keyHash) > 0 && myNodeId.compareTo(keyHash) <= 0) {
//                    providerData.put(key, value);
//                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.INSERT_REPL,
//                            key, value);
//                    Log.i(TAG, "Key Belong to " + myPort);
//                } else {
////                    callOwnerOfKey(key, value);
//                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.INSERT_PROP,
//                            key, value);
//                }
//            } else {
//                printRing("From @ query method myPort == firstKey " + myPort);
//                String lastNodeId = genHash(chordRing.lastKey());
//                if ((lastNodeId.compareTo(keyHash) > 0 && myNodeId.compareTo(keyHash) > 0) || myNodeId.compareTo(keyHash) <= 0) {
//                    providerData.put(key, value);
//                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.INSERT_REPL,
//                            key, value);
//                    Log.i(TAG, "Last Key Belong to " + myPort);
//                } else {
//                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.INSERT_PROP,
//                            key, value);
////                    callOwnerOfKey(key, value);
//                }
//            }
        }catch (NoSuchAlgorithmException e){
            Log.e(TAG, e.toString());
        }

        Log.v("insert ", values.toString());
		return null;
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

    public Void handleInsert(SimpleDhtMessage message) {
        String[] pair = message.msgData.split(Constants.SEP);
        String key = pair[0], value = pair[1];

        String keyHash = null;
        try {
            keyHash = genHash(key);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if(message.msgType.equals(Constants.INSERT_PROP)) {
            // If the current node is first node
            if (myPort.equals(chordRing.firstKey())) {
                String tmpLastId = chordRing.get(chordRing.lastKey());
                // If key belongs to me
                if (tmpLastId.compareTo(keyHash) < 0 || keyHash.compareTo(myNodeId) <= 0) {
                    storeMessage(Constants.INSERT, key, value);
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_REPL,
                            key, value);
                } else if (keyHash.compareTo(myNodeId) > 0) {
                    // Propagate to next node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.higherKey(myPort))),
                            key, value);
                }
            }// If the current node is last node
            else if (myPort.equals(chordRing.lastKey())) {
                String tmpPrevId = chordRing.get(chordRing.lowerKey(myPort));
                // If key belongs to me
                if (tmpPrevId.compareTo(keyHash) < 0 && keyHash.compareTo(myNodeId) <= 0) {
                    storeMessage(Constants.INSERT, key, value);
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_REPL,
                            key, value);
                } else if (keyHash.compareTo(myNodeId) > 0) {
                    // Propagate to next(first) node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.firstKey())),
                            key, value);

                } else {
                    // Propagate to prev node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.lowerKey(myPort))),
                            key, value);
                }
            }// If the current node is middle node
            else {
                String tmpPrevId = chordRing.get(chordRing.lowerKey(myPort));
                // If key belongs to me
                if (tmpPrevId.compareTo(keyHash) < 0 && keyHash.compareTo(myNodeId) <= 0) {
                    storeMessage(Constants.INSERT, key, value);
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_REPL,
                            key, value);
                } else if (keyHash.compareTo(myNodeId) > 0) {
                    // Propagate to next node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.higherKey(myPort))),
                            key, value);

                } else {
                    // Propagate to prev node
                    new InsertClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,
                            Constants.INSERT_PROP,
                            String.valueOf(Integer.parseInt(chordRing.lowerKey(myPort))),
                            key, value);
                }

            }
        }else if(message.msgType.equals(Constants.INSERT_REQ)){
            storeMessage(Constants.INSERT_REPL, key, value);
        }
        return null;
    }

//    public Void createChordRing(){
//        try {
//            for (String i : EMU_PORTS) {
//                chordRing.put(i, genHash(i));
//            }
//        }catch (NoSuchAlgorithmException e){
//            Log.e(TAG, e.toString());
//        }
//        return null;
//    }

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
            Log.i(TAG, "Node update request recieve by " + myPort+ " list"+ message.msgData);

            if(message.msgData.contains(Constants.PIPE)) {
                Log.i(TAG, "Msg Data "+message.msgData);
                String[] list = message.msgData.split(Constants.PIPE);
                for (int i = 0; i < list.length; i++){
                    Log.i(TAG, "Split Data "+list[i]);
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

        return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub
        MatrixCursor cursor = new MatrixCursor(
                new String[] {"key", "value"}
        );

        if(selection.equals("@")){

            for(String item: providerData.keySet()) {
                cursor.newRow()
                        .add("key", item)
                        .add("value", providerData.get(item));
            }


        }else if(selection.equals("*")){

            cursor.newRow()
                    .add("key", selection)
                    .add("value", providerData.get(selection));
        }else{
            Log.i(TAG, "Find " + selection + " " + providerData.get(selection));

            if(providerData.containsKey(selection)) {
                cursor.newRow()
                        .add("key", selection)
                        .add("value", providerData.get(selection));
            }else{

            }
        }

        return cursor;

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
            Log.i(TAG, "Data item " + item);
            data += item + Constants.PIPE;
        }
        Log.i(TAG, "Data Len " + data.length());
        data = data.substring(0, data.length() - 2);
        Log.i(TAG, "Send to source to update list "+srcPort+" Node list"+data);
        new JoinClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, Constants.JOIN_UPDATE, srcPort,
                data);
//    }
        return null;
    }


    private Void decodeMessage(String[] msg){
        if(msg[0].contains("join")) {
            SimpleDhtMessage message = new SimpleDhtMessage(msg[0], msg[1], msg[2]);
            handleJoin(message);

        }else if(msg[0].contains(Constants.INSERT)){
            SimpleDhtMessage message = new SimpleDhtMessage(msg[0], Constants.BLANK,
                    msg[1]+ Constants.SEP+msg[2]);
            handleInsert(message);

        }else if(msg[0].equals((Constants.DELETE))){
            SimpleDhtMessage message = new SimpleDhtMessage(msg[0], Constants.BLANK,
                    msg[1]+ Constants.SEP+msg[2]);
            handleDelete(message);
        }

        return null;
    }

    private void printRing(String toPrint){
        Log.i(TAG, "Print Ring "+toPrint+chordRing.keySet().toString());
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        private final String TAG = SimpleDynamoProvider.class.getSimpleName();

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
                    String[] msg = inp.split(Constants.SEP);

                    decodeMessage(msg);


                    bfrRdr.close();
                    socket.close();
                    Log.i(TAG, "Ring "+chordRing.keySet().toString());
                }

            }
            catch (IOException e) {
                e.printStackTrace();
            }


            return null;
        }

    }


    private class InsertClientTask extends AsyncTask<String, Void, Void> {

        private final String TAG = SimpleDynamoProvider.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {

//            SimpleDhtMessage message = new SimpleDhtMessage(msgs[0], msgs[1], msgs[2]);

            if(msgs[0].equals(Constants.INSERT_REPL)){
                Log.i(TAG, Constants.INSERT_REPL);
                String[] WRITER_PORTS = new String[2];

                if(myPort.equals(chordRing.lastKey())){
                    WRITER_PORTS[0] = chordRing.firstKey();
                }else {
                    WRITER_PORTS[0] = chordRing.higherKey(myPort);
                }

                if(WRITER_PORTS[0].equals(chordRing.lastKey())){
                    WRITER_PORTS[1] = chordRing.firstKey();
                }else {
                    WRITER_PORTS[1] = chordRing.higherKey(WRITER_PORTS[0]);
                }


                Socket socket;
                for(int i = 0; i < 2; i++) {
                    try {

                        socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(WRITER_PORTS[i])*2);

                        BufferedWriter bfrWtr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                        bfrWtr.write(Constants.INSERT_REQ + Constants.SEP + msgs[1] + Constants.SEP + msgs[2]);
                        bfrWtr.flush();
                        socket.close();

                    }catch (ConnectException e) {
                        continue;

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }else if(msgs[0].equals(Constants.INSERT_PROP)){
                Log.i(TAG, Constants.INSERT_PROP);

                Socket socket;

                try {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1])*2);

                    BufferedWriter bfrWtr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bfrWtr.write(Constants.INSERT_PROP + Constants.SEP + msgs[2] + Constants.SEP + msgs[3]);
                    bfrWtr.flush();
                    socket.close();

                }catch (IOException e) {
                    e.printStackTrace();
                }

            }




            return null;
        }
    }

    private class JoinClientTask extends AsyncTask<String, Void, Void> {

        private final String TAG = SimpleDynamoProvider.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {

            Log.e(TAG, "SimpleMessage : "+msgs[0]+ msgs[1]+msgs[2]);
//            SimpleDhtMessage message = new SimpleDhtMessage(msgs[0], msgs[1], msgs[2]);

            Socket socket;
            for(int i = 0; i < 5; i++) {
                try {

                    socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORTS[i]));

                    BufferedWriter bfrWtr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                    bfrWtr.write(msgs[0] + Constants.SEP + msgs[1] + Constants.SEP + msgs[2]);
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

        private final String TAG = SimpleDynamoProvider.class.getSimpleName();
        @Override
        protected Void doInBackground(String... msgs) {

            Log.e(TAG, "SimpleMessage : "+msgs[0]+ msgs[1]+msgs[2]);
//            SimpleDhtMessage message = new SimpleDhtMessage(msgs[0], msgs[1], msgs[2]);

            Socket socket;
            String nextNode = null;
            if(myPort.equals(chordRing.lastKey())){
                nextNode = chordRing.firstKey();
            }else {
                nextNode = chordRing.higherKey(myPort);
            }


            try {
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextNode)*2);

                BufferedWriter bfrWtr = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bfrWtr.write(msgs[0] + Constants.SEP + msgs[1] + Constants.SEP + msgs[2]);
                bfrWtr.flush();
                socket.close();

            }
            catch (IOException e) {
                e.printStackTrace();
            }


            return null;
        }
    }

}
