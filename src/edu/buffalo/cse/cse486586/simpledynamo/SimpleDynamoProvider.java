package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map.Entry;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	
	public static String myOwnPort=null;
	public static String myPort=null;
	static final int SERVER_PORT = 10000;
	public static String myHash = null;
	private SQLiteDatabase database;
	MyDBHelper DBHelper;
	public static final String AUTHORITY ="edu.buffalo.cse.cse486586.simpledynamo.provider";
	public static final Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY);
	public static final String table_name= "OBJECTS";
	private HashMap<String,Info> map=new HashMap<String,Info>();
	private String pList[] = new String[2];
	String list[]= new String[2];
	String TAG = "MY DEBUGGER";
	String TAG2 = "STAR!";
	Object ob = new Object();
	boolean flag = false;
	
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		Log.d("DELETE","request for delete received at port "+myPort);
		String query_statement= "SELECT key, value FROM "+table_name+" WHERE key=?";
		String[] sel=new String[1];
		sel[0]=selection;
		Cursor cursor = database.rawQuery(query_statement,sel);
		Info info;
		String msg = "DELETE"+selection;
		for(Entry<String,Info> e:map.entrySet()){
			info = e.getValue();
			Log.d("DELETE","request for delete forwarded to port "+info.getMyPort());
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, info.getMyPort());
		}
		
		if(cursor.getCount()>0){
			Log.d("DELETE","key "+selection+" found at initiator port "+myPort+" and deleted");
			String where = "key=?";
			return database.delete(table_name, where, selection == "*" ? null : new String[] {selection});
		}else{
			return 0;
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		String msg=null;
	
		Info info;
		ArrayList<String> temp = new ArrayList<String>();
		for(Entry<String,Info> e:map.entrySet()){
			info = e.getValue();
			
			if(info.containsKey(key)){
				Log.d(TAG, "checking in "+e.getKey());
				Log.d(TAG, "INSIDE");
				temp.add(info.myPort);
				temp.add(info.mySucc1);	
				temp.add(info.mySucc2);
				msg = key+"####"+value+"####"+info.myPort;
				for(String s:temp){
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, s);
					
				}
			
			}	

		}
		
		return Uri.withAppendedPath(CONTENT_URI, String.valueOf(value));
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		DBHelper = new MyDBHelper(getContext());
		database = DBHelper.getWritableDatabase();
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myOwnPort = String.valueOf((Integer.parseInt(portStr) * 2));
		
		switch(myOwnPort){
		case "11108":
			myPort = "5554";
			break;
		case "11116":
			myPort = "5558";
			break;
		case "11120":
			myPort = "5560";
			break;
		case "11124":
			myPort = "5562";
			break;
		case "11112":
			myPort = "5556";
			break;
		}
		
		map.put("5554", new Info("5554","5558","5560","5556", "5562"));
		map.put("5558", new Info("5558","5560","5562","5554", "5556"));
		map.put("5560", new Info("5560","5562","5556","5558","5554"));
		map.put("5562", new Info("5562","5556","5554","5560","5558"));
		map.put("5556", new Info("5556","5554","5558","5562","5560"));
	
		if(DBHelper.getFlag()){
			flag = true;
			new beginRecovery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);
		}else{
			Log.d("RECOVERY","Fresh install!");
		}
		
		
		try{
			
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e("Error", "Can't create a ServerSocket");
		}	
	return true;	
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		Cursor cursor=null;
		if(selection.contains("QUERY*")){
			cursor = database.rawQuery("SELECT key, value FROM "+table_name,null);
			return cursor;
		}else if(selection.contains("QUERY?")){
			selection = selection.replace("QUERY?","");
			String query_statement= "SELECT key, value FROM "+table_name+" WHERE key=?";
			String[] sel=new String[1];
			sel[0]=selection;
			cursor = database.rawQuery(query_statement,sel);
			Log.d(TAG,"Port "+myPort+"computing the result for key "+selection+ " the cursor count is "+cursor.getCount());
			return cursor;
		}else if(selection.equals("@")){
			Log.d(TAG, "query for @ received at "+myPort);
			cursor = database.rawQuery("SELECT key, value FROM "+table_name,null);
			if(cursor.getCount()==0)
				Log.d(TAG, "database empty!!");
			return cursor;
		}else if(selection.equals("*")){
			cursor = database.rawQuery("SELECT key, value FROM "+table_name,null);
			Info info;
			String msg = "QUERY*";
			String result = Serialized(cursor);
			if(result==null)
				result = "";
			for(Entry<String,Info> e:map.entrySet()){
				info = e.getValue();
				String line = "";
				try{
					Log.d("STAR","query request for * generated at port: "+myPort+" and sent to "+info.getMyPort());
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(info.getMyPort())*2);
					PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
					printwriter.write(msg+"\n");  //write the message to output stream
					printwriter.flush();
					BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					line = br.readLine();
					line = line.trim();
					br.close();
					//cursor = Deserialized(result);
					Log.d("STAR", "Response for * received at the initiator port: "+myPort+" from "+info.getMyPort());
					printwriter.close(); //close the output stream
					socket.close(); //close connection
					
					}catch(Exception EX){
						Log.e("STAR","node "+info.getMyPort()+" had failed during *query");
						continue;
						//more code here!
					}
					
				if(!line.isEmpty()&&!line.equals(""))
				result = result + " " + line;
				result = result.trim();
			}
		cursor = Deserialized(result);
		return cursor;
			
		}else{
			String key = selection;
			String line = null;
			
			String query_statement= "SELECT key, value FROM "+table_name+" WHERE key=?";
			String[] sel=new String[1];
			sel[0]=selection;
			cursor = database.rawQuery(query_statement,sel);
			if(cursor.getCount()>0){
				return cursor;
			}else{
			try{
			Info info;
			String msg = "QUERY?"+key;
			for(Entry<String,Info> e:map.entrySet()){
				info = e.getValue();
				if(info.containsKey(key)){
					Socket socket=null;
					try{
						Log.d(TAG,"query request for key "+key+" generated at port: "+myPort+" and sent to "+info.getMyPort());
						socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(info.getMyPort())*2);
						PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
						printwriter.write(msg+"\n");  //write the message to output stream
						printwriter.flush();
						BufferedReader br=null;
						br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						line = br.readLine();
						cursor = Deserialize(line);
						Log.d(TAG, "Response for key "+key+" received at the initiator port: "+myPort+" from "+info.getMyPort()+" response is "+line);
						printwriter.close(); //close the output stream
						br.close();
						socket.close(); //close connection
						return cursor;
						}catch(Exception Ex){ //node failure
//							printwriter.close(); //close the output stream
//							br.close();
//							socket.close();
							Log.d("key","Node failed: sending query to its successor");
							socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(info.getMySucc1())*2);
							PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
							printwriter.write(msg+"\n");  //write the message to output stream
							printwriter.flush();
							BufferedReader br=new BufferedReader(new InputStreamReader(socket.getInputStream()));
							line = br.readLine();
							cursor = Deserialize(line);
							Log.d("key", "Response for key "+key+" received at the initiator port: "+myPort+" from "+info.getMySucc1());
							printwriter.close(); //close the output stream
							br.close();
							socket.close(); //close connection
							return cursor;
						}
						
						
				}
			}
			
			}catch(Exception e){
				Log.e("SHA error", e.toString());
			}
				
			return cursor;
		}
		}
	}
	private Cursor Deserialized(String str){
		String[] a = str.split("\\s");
		String[] colNames = {"key", "value"};
		MatrixCursor cu = new MatrixCursor(colNames);
		
		for(int i=0;i<a.length;i++){
			String[] temp = a[i].trim().split("\\|");
			cu.addRow(new String[]{temp[0],temp[1]});
		}
		return cu;
	}
	private Cursor Deserialize(String str){
		String[] a = str.split("\\|");
		String[] colNames = {"key", "value"};
		MatrixCursor cu = new MatrixCursor(colNames);
		cu.addRow(new String[]{a[0],a[1]});
		return cu;
	}
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			/*
			 * TODO: Fill in your server code that receives messages and passes them
			 * to onProgressUpdate().
			 */
			ServerSocket serverSocket = sockets[0];
			Socket clientSocket;
			InputStreamReader inputStreamReader;
			BufferedReader bufferedReader;
			String msg=null;
			
			while (true) {
				try {
					clientSocket = serverSocket.accept();   //accept the client connection
					inputStreamReader = new InputStreamReader(clientSocket.getInputStream());
					bufferedReader = new BufferedReader(inputStreamReader); //get the client message
					msg = bufferedReader.readLine();
					//Log.d(TAG, "received msg "+msg);
					if(flag){
						synchronized(ob){
							ob.wait();
						}
					}
					
					if(msg.contains("#")){
						
						String a[] = msg.trim().split("####");
						String key = a[0];
						String value = a[1];
						String port = a[2];
						String query_statement= "SELECT key, value FROM "+table_name+" WHERE key=?";
						String[] sel=new String[1];
						sel[0]=key;
						Cursor cursor = database.rawQuery(query_statement,sel);
						if(cursor.getCount()==0){
						ContentValues cv = new ContentValues();
						Log.d("INSERT", "key = "+key+" value = "+value+" port = "+port);
						cv.put("key", a[0]);
						cv.put("value", a[1]);
						cv.put("port", port);
						database.insert(table_name, null, cv);
						}else{
							database.delete(table_name, "key=?", new String[]{key});
							ContentValues cv = new ContentValues();
							cv.put("key", a[0]);
							cv.put("value", a[1]);
							cv.put("port", port);
							database.insert(table_name, null, cv);
							//Log.d("INSERT", "insert UPDATE: key = "+key+" value = "+value+" port = "+port);
							Log.d(TAG,"New version for key "+key+" received. new value is "+value);
						}
						Log.d(TAG, "inserted key "+key+" value "+a[1]+" port "+port);

					}else if(msg.contains("QUERY*")){
						Uri mUri = buildUri("content", AUTHORITY);
						Log.d("STAR","Port "+myPort+" received * query request");
						ContentResolver cr = getContext().getContentResolver();
						Cursor resultCursor = cr.query(mUri, null, msg, null, null);
						PrintWriter wr = new PrintWriter(clientSocket.getOutputStream(), true);
						String line = Serialized(resultCursor);
						Log.d("STAR","Port "+myPort+" responded * query request response is "+line);
						wr.write(line);  //write the message to output stream
						wr.flush();
						wr.close();
					}else if(msg.contains("QUERY?")){
						String temp = msg.replace("QUERY?", "");
						Log.d(TAG,"Port "+myPort+" received key query request for key = "+temp);
						Uri mUri = buildUri("content", AUTHORITY);
						ContentResolver cr = getContext().getContentResolver();
						Cursor resultCursor = cr.query(mUri, null, msg, null, null);
						PrintWriter wr = new PrintWriter(clientSocket.getOutputStream(), true);
						String line = Serialize(resultCursor);
						Log.d(TAG,"Port "+myPort+" responded key query request response is "+line);
						wr.write(line);
						wr.flush();
						wr.close();
					}

		
					else if(msg.contains("SUCC")){//recovery msgs
						Log.d("RECOVERY","received (successor)recovery request at "+myPort);
						msg = msg.replace("SUCC", ""); //msg is the port number
						Uri mUri = buildUri("content", AUTHORITY);
						ContentResolver cr = getContext().getContentResolver();
						Log.d("RECOVERY", "searching for port "+msg);
						String query_statement= "SELECT * FROM "+table_name+" WHERE port=?";
						String[] sel=new String[1];
						sel[0]=msg;
						Cursor resultCursor = database.rawQuery(query_statement,sel);
						if(resultCursor.getCount()==0){
							Log.d("RECOVERY", "no result received");
						}
						Log.d("RECOVERY", "calling serialized recovery");
						String line = SerializedRecovery(resultCursor);
						Log.d("RECOVERY","successor Port "+myPort+" responded recovery request; response is "+line);
						PrintWriter wr = new PrintWriter(clientSocket.getOutputStream(), true);
						wr.write(line+"\n");
						wr.flush();
						wr.close();
						}
					else if(msg.contains("PRED")){
						Log.d("RECOVERY","received (pred)recovery request at "+myPort);
						msg = msg.replace("PRED", "");
						Uri mUri = buildUri("content", AUTHORITY);
						ContentResolver cr = getContext().getContentResolver();
						String query_statement= "SELECT * FROM "+table_name+" WHERE port=?";
						String[] sel=new String[1];
						sel[0]=myPort;
						Cursor resultCursor = database.rawQuery(query_statement,sel);
						String line = SerializedRecovery(resultCursor);
						Log.d("RECOVERY","predecessor Port "+myPort+" responded recovery request; response is "+line);
						PrintWriter wr = new PrintWriter(clientSocket.getOutputStream(), true);
						wr.write(line+"\n");
						wr.flush();
						wr.close();
					}
					else if(msg.contains("DELETE")){
						msg = msg.replace("DELETE", "");
						String query_statement= "SELECT key, value FROM "+table_name+" WHERE key=?";
						String[] sel=new String[1];
						sel[0]=msg;
						Cursor cursor = database.rawQuery(query_statement,sel);
						if(cursor.getCount()>0){
							Log.d("DELETE","key "+msg+" found in "+myPort+" and deleted locally");
							String where = "key=?";
							database.delete(table_name, where, msg == "*" ? null : new String[] {msg});
						}else{
							Log.d("DELETE","key "+msg+" not found.");
						}
					}
					clientSocket.close();
				}catch(Exception e){
					Log.e("SERVERTASK error", e.toString()+e.getMessage());
					e.printStackTrace();
				}
				
			}
		}
	}
		
	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
				String msg = msgs[0];
				try{
					Log.d(TAG, "CLIENTTASK insert request sent to port "+msgs[1]);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1])*2);
					PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
					printwriter.write(msg);  //write the message to output stream
					printwriter.flush();
					printwriter.close(); //close the output stream
				    socket.close(); //close connection
				}catch(Exception e){
					Log.e("CLIENTTASK ERROR", e.toString());
				}
				
			
			
			return null;

		}
	}
	
	private String Serialized(Cursor cu){
		String key = null;
		String value = null;
		String result = "";
		if (cu.moveToFirst()){
			while(!cu.isAfterLast()){
				key = cu.getString(cu.getColumnIndex("key"));
				value = cu.getString(cu.getColumnIndex("value"));
				if(result.equals(""))
				result = key + "|" + value;
				else
				result = result + " " + key + "|" + value;
				cu.moveToNext();
			}
		}
		
		if(result.equals("")){
			Log.d(TAG,"result is empty in query");
		}
		
		return result;
	}
	private String SerializedRecovery(Cursor cu){
		Log.d("RECOVERY", "cursor size is "+cu.getCount());
		String key = null;
		String value = null;
		String port = null;
		String result = "";
		if (cu.moveToFirst()){
			while(!cu.isAfterLast()){
				key = cu.getString(cu.getColumnIndex("key"));
				value = cu.getString(cu.getColumnIndex("value"));
				port = cu.getString(cu.getColumnIndex("port"));
				if(result.equals("")){
				result = key + "|" + value + "|" + port;
			//	Log.d("GREENDAY", "first while, key = "+key+" value = "+value+" port = "+port);
				}else{
				result = result + " " + key + "|" + value + "|" + port;
				//Log.d("GREENDAY", "for other while, key = "+key+" value = "+value+" port = "+port);
				}cu.moveToNext();
			}
		}
	//	Log.d("GREENDAY", "inside SERIALIZED RECOVERY, result = "+result);
		if(result.equals("")){
			Log.d("RECOVERY","result is empty in recovery query");
		}
		//Log.d("RECOVERY", "inside serialized recovery, resukt is "+result);
		return result;
	}
	private String Serialize(Cursor cu){
		String key = null;
		String value = null;
		if (cu.moveToFirst()){
			while(!cu.isAfterLast()){
				key = cu.getString(cu.getColumnIndex("key"));
				value = cu.getString(cu.getColumnIndex("value"));
				cu.moveToNext();
			}
		}
		cu.close();
		if(key==null||value==null){
			Log.d(TAG,"Result is null");
		}
		String result = key+"|"+value;
		
		return result;
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
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }


    
    private class beginRecovery extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			//msgs[0]-->port
			
			
			//recover from successor
			String msg = "SUCC"+msgs[0];
			Log.d("RECOVERY", "Recovery Detected for avd "+myPort);
			String line=null;
			Info info = map.get(myPort);
			try{
				Log.d("RECOVERY", "sending to successor "+info.getMySucc1());
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(info.getMySucc1())*2);
				PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
				printwriter.write(msg+"\n");  //write the message to output stream
				printwriter.flush();
				BufferedReader br= new BufferedReader(new InputStreamReader(socket.getInputStream()));
				line = br.readLine();
				Log.d("RECOVERY", "Received response from the successor "+info.getMySucc1()+ " response is "+line);
				br.close();
				printwriter.close(); //close the output stream
				socket.close(); //close connection
			}catch(Exception e){
				Log.e("RECOVERY ASYNCTASK ERROR in succ", e.toString());
			}
			
			
			//recover from pred:
			String result = line.trim();
			ArrayList<String> temp = new ArrayList<String>();
			temp.add(info.getMyPred1());
			temp.add(info.getMyPred2());
			msg = "PRED"+msgs[0];
			
			for(String s:temp){
				try{
					Log.d("RECOVERY", "sending to predecessor "+s);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(s)*2);
					PrintWriter printwriter = new PrintWriter(socket.getOutputStream(),true);
					printwriter.write(msg+"\n");  //write the message to output stream
					printwriter.flush();
					BufferedReader br= new BufferedReader(new InputStreamReader(socket.getInputStream()));
					line = br.readLine();
					Log.d("RECOVERY", "received response from predec "+s);
					br.close();
					printwriter.close(); //close the output stream
					socket.close(); //close connection
				}catch(Exception e){
					Log.e("RECOVERY ASYNCTASK ERROR in predec", e.toString());
				}
				result = result + " " + line.trim();
				result = result.trim();
				}
				result = result.trim();
				
				Log.d("RECOVERY","The total result found is "+result);
				
				//deleting local db
				database.delete(table_name, null, null);
				Log.d(TAG, "deleting local DB");
				if(result==null||result.isEmpty()||result.equals("")||result.equals(null)){
					
				}else{	
				//insertion:
				String[] temp1 = result.split("\\s");
				
				for(int i =0;i<temp1.length;i++){
					String query_statement= "SELECT * FROM "+table_name+" WHERE key=?"; 
					String[] sel=new String[1];
					sel[0]=temp1[i].split("\\|")[0];
					Cursor cursor = database.rawQuery(query_statement,sel);
					if(cursor.getCount()==0){
					ContentValues cv = new ContentValues();
					cv.put("key", temp1[i].split("\\|")[0]);
					cv.put("value", temp1[i].split("\\|")[1]);
					cv.put("port", temp1[i].split("\\|")[2]);
					Log.d("INSERT", "insert during recovery: key = "+temp1[i].split("\\|")[0]+" value = "+temp1[i].split("\\|")[1]+" port = "+temp1[i].split("\\|")[2]);
					database.insert(table_name, null, cv);
					}
					}
				}
				flag = false;
				synchronized(ob){
					ob.notifyAll();
				}
				return null;
		}
	}
}
