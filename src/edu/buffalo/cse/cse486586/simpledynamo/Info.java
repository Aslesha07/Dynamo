package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.util.Log;

public class Info {
	
	String myPort;
	String mySucc1;
	String mySucc2;
	String myPred1;
	String myPred2;
	
	public Info(String myPort, String mySucc1, String mySucc2, String myPred1, String myPred2){
		this.myPort= myPort;
		this.mySucc1=mySucc1;
		this.mySucc2=mySucc2;
		this.myPred1=myPred1;
		this.myPred2=myPred2;
	}
	
	public String getMyPort() {
		return myPort;
	}
	public void setMyPort(String myPort) {
		this.myPort = myPort;
	}
	public String getMySucc1() {
		return mySucc1;
	}
	public String getMySucc2() {
		return mySucc2;
	}
	public void setMySucc1(String mySucc) {
		this.mySucc1 = mySucc;
	}
	public void setMySucc2(String mySucc) {
		this.mySucc2 = mySucc;
	}
	public String getMyPred1() {
		return myPred1;
	}
	public String getMyPred2() {
		return myPred2;
	}
	public void setMyPred1(String myPred) {
		this.myPred1 = myPred;
	}
	public void setMyPred2(String myPred) {
		this.myPred2 = myPred;
	}
	
	
	public boolean containsKey(String key){
		try{
		if(genHash(this.myPred1).compareTo(genHash(this.myPort))>0){
			if((genHash(key).compareTo(genHash(this.myPred1))>0)||(genHash(key).compareTo(genHash(this.myPort))<0)){
				Log.d("MY DEBUGGER", "key "+key+" lies between 5560 and 5562");
				return true;
			}else
				return false;
		}else{
			if(genHash(key).compareTo(genHash(myPort))<0&&genHash(key).compareTo(genHash(myPred1))>0){
				Log.d("MY DEBUGGER", "key "+key+" lies between "+myPort+" and "+myPred1);
				return true;
			}else
				return false;
		}
		}catch(Exception e){
			Log.e("SHA excpetion in INFO", e.toString());
		}
		return false;
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
	

}
