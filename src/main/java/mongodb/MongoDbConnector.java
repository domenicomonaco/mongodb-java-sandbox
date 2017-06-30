/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mongodb;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoIterable;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MongoDbConnector {

	// CHECKSTYLE:ON

	private String host= "localhost";
	private int port = 27017;
	private String dbname;

	public MongoClient mongoClient;
	/**
	 * Constructor
	 */
	public MongoDbConnector() {}

	public boolean isCollectionExists(DB db, String collectionName) 
	{
		DBCollection table = db.getCollection(collectionName);
		return (table.count()>0)?true:false;
	}

	/**
	 * Disable MongoDb logging into console
	 */
	public void disableLoggingConsole() {
		//disable MongoDb logging into console
		Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
		mongoLogger.setLevel(Level.SEVERE);
	}

	public void connect(){
		this.mongoClient = new MongoClient(host, port);
	}


	/**
	 * print into console the list of avaliable database
	 */
	public void print_ListDatabase() {
		System.out.println("# LIST OF DATABASE");
		int i=0;
		//Get list of Database
		MongoIterable<String> a = mongoClient.listDatabaseNames();
		for(String s: a){
			System.out.println(i + ": " + s);
			i++;
		}
		System.out.println("\n");
	}


	public boolean ifExistDB(String dbName) {	
		return mongoClient
				.listDatabaseNames()
				.into(new ArrayList<String>())
				.contains(dbName);
	}

	public void print_ifExistDB(String dbName) {
		System.out.println("# ifExistDB: "+ dbName + " = " + this.ifExistDB(dbName));
	}


	public boolean ifExistCollection(String dbName, String collectionName) {
		if(ifExistDB(dbName)==true) {
			return mongoClient.getDatabase(dbName)
					.listCollectionNames()
					.into(new ArrayList<String>()).contains(collectionName);
		}else {
			print_ifExistDB(dbName);
			return false;
		}	
	}

	public void print_ifExistCollection(String dbName, String collectioName) {
		System.out.println("# ifExistCollection: "
				+ dbName 
				+ "." 
				+ collectioName
				+ " = " 
				+ this.ifExistCollection(dbName,collectioName));
	}

	// CHECKSTYLE:ON
	
	public static void main(final String[] args) throws UnknownHostException {

		MongoDbConnector TestDB = new MongoDbConnector();

		TestDB.disableLoggingConsole();
		TestDB.connect();

		TestDB.print_ListDatabase();

		TestDB.print_ifExistDB("TEST");
		TestDB.print_ifExistDB("ow");
		TestDB.print_ifExistDB("admin");

		TestDB.print_ifExistCollection("ow","test");	    	
		TestDB.print_ifExistCollection("ow","hello");

		TestDB.mongoClient.close();
	}
}


