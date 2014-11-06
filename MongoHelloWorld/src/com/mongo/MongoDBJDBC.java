package com.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.util.JSON;

import java.util.Set;

public class MongoDBJDBC{
	
	DB db = null;
	DBCollection coll = null;
	
	public boolean connectToDB(){
		String myUserName = "chetana";
		   char myPassword[]="pass1234".toCharArray();
		   boolean auth=false;
	      try{   
			 // To connect to mongodb server
	         MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	         // Now connect to your databases
	         db = mongoClient.getDB( "iGo" );
			 System.out.println("Connect to database successfully");
	         auth = db.authenticate(myUserName, myPassword);
			 System.out.println("Authentication: "+auth);
			 coll = db.getCollection("test");
			 return auth;
	      }catch(Exception e){
		     System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		     return auth;
		  }
	}
	
	public boolean insert(String data){
		
		// convert JSON to DBObject directly
		DBObject dbObject = (DBObject) JSON.parse(data);
	    coll.insert(dbObject);
		
		return false;
	}
	
	public void insertMultiple(){ 
		for(int i=1;i<=100;i++){
			coll.insert(new BasicDBObject("i", i));
		}
	}
	
	public void findFirstDoc(){
		DBObject myDoc = coll.findOne();
		System.out.println(myDoc);
	}
	
	public String read(){
		DBCursor cursor = coll.find();
		try {
		   while(cursor.hasNext()) {
		       System.out.println(cursor.next());
		   }
		} finally {
		   cursor.close();
		}
		return "";
	}
	
	public void showAllCollectoins(){
		Set<String> colls = db.getCollectionNames();
		   for(String s : colls) {
			   System.out.println(s);
		   }
	}
	
	public void search(){
		BasicDBObject query = new BasicDBObject("i", 22);

		DBCursor cursor = coll.find(query);
		DBObject dd = cursor.getQuery();
		System.out.println(dd);

		try {
		   while(cursor.hasNext()) {
		       System.out.println(cursor.next());
		   }
		} finally {
		   cursor.close();
		}
	}
	
	public void search(String key, String value){
		BasicDBObject query = new BasicDBObject(key, value);

		DBCursor cursor = coll.find(query);
		System.out.println(cursor.count());

		try {
		   while(cursor.hasNext()) {
		       System.out.println(cursor.next());
		   }
		} finally {
		   cursor.close();
		}
	}
	
	public void drop(){
		//DBObject obj = (DBObject) JSON.parse("{}");
		coll.drop();
	}
	
	public void testFunction(){
		String user="Chetana";
		String password = "pass123";
		int loginToken = 0;
		String data = "{\"name\":\"" + user + "\",\"password\":\"" + password + "\"}";
		DBObject queryObj = (DBObject) JSON.parse(data);
		coll = db.getCollection("myusers");
		DBCursor cursor = coll.find(queryObj);
		System.out.println(coll.count());
		System.out.println(cursor);
		try{
			while(cursor.hasNext()) {
				//loginToken = Integer.parseInt((cursor.next().toString()));
				//System.out.println(loginToken);
				  //loginToken =
				
				loginToken = (int)Float.parseFloat((cursor.next().get("userid").toString()));
				//loginToken = cursor.next().get("userid");
				System.out.println("DATA " + loginToken);
			}
		}
		catch(MongoException e){
			System.out.println("Can't get user");
		}
	}
	
   public static void main( String args[] ){
	   MongoDBJDBC mongo = new MongoDBJDBC();
	   mongo.connectToDB();/*
	   //getting collections
	   mongo.showAllCollectoins();
	   //drop
	   System.out.println("Drop");
	   mongo.drop();
	   
	   //insert
	   String data="{ \"name\":\"MongoDB\",\"type\":\"Database\",\"count\":1,\"info\":{x:203,y:102}}";
	   mongo.insert(data);
	   data="{ \"name\":\"SQLDB\",\"type\":\"Database\",\"count\":1,\"info\":{x:203,y:102}}";
	   mongo.insert(data);
	   //display first
	   System.out.println("display first:");
	   mongo.findFirstDoc();
	   //insert more
	   mongo.insertMultiple();
	   //show all
	   System.out.println("show all");
	   mongo.read();
	   System.out.println("Search One");
	   mongo.search();
	   //System.out.println("Search One");
	   mongo.search("name","MongoDB");*/
	   mongo.testFunction();
	   
   }
}
