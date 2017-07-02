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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.util.JSON;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.bson.Document;

public class MongoDbConnector {

	private String host = "localhost";
	
	private int port = 27017;
	
	public MongoClient client;
	
	/**
	 * Constructor provide mongoDb connection and setting some options
	 */
	public MongoDbConnector() {
		this.disableLoggingConsole();
		this.connect();
	}
	
	/**
	 * Disable MongoDb logging into console
	 */
	public void disableLoggingConsole() {
		//disable MongoDb logging into console
		Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
		mongoLogger.setLevel(Level.SEVERE);
	}
	 
	/**
	 * Create connection and MongoDb Client object
	 */
	private void connect(){
		if(this.client==null) {
			this.client = new MongoClient(host, port);
			}
	}
	
	public boolean ifExistDB(String dbName) {	
		return client
				.listDatabaseNames()
				.into(new ArrayList<String>())
				.contains(dbName);
	}

	public boolean ifExistDifferentCaseDB(String dbName) {	
		ArrayList<String> listDb= client
				.listDatabaseNames()
				.into(new ArrayList<String>());
		for(String s: listDb){
			if(s.toLowerCase().equals(dbName.toLowerCase())) return true;
		}
		
		return false;
	}

	
	public void print_ifExistDB(String dbName) {
		System.out.println("# ifExistDB: "+ dbName + " = " + this.ifExistDB(dbName));
	}

	/**
	 * print into console the list of avaliable database
	 */
	public void print_listDatabase() {
		System.out.println("# LIST OF DATABASE");
		int i=0;
		//Get list of Database
		MongoIterable<String> a = client.listDatabaseNames();
		for(String s: a){
			System.out.println(i + ": " + s);
			i++;
		}
		
		System.out.println("\n");
	}

	public boolean ifExistCollection(String dbName, String collectionName) {
		if(ifExistDB(dbName)==true) {
			return client.getDatabase(dbName)
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
	
	public boolean ifDatabaseEmpty(String dbName) {
		
		System.out.println(this.client.getDatabase(dbName).listCollectionNames()
	    .into(new ArrayList<String>()).size());
		
		return true;
	}

	
	public MongoClient getClient() {
		return this.client;
	}
	
	// CHECKSTYLE:ON
	
	public static void main(final String[] args) throws UnknownHostException {

		MongoDbConnector TestDB = new MongoDbConnector();

		TestDB.print_listDatabase();

		TestDB.print_ifExistDB("TEST");
		TestDB.print_ifExistDB("ow");
		TestDB.print_ifExistDB("admin");
		
		TestDB.ifDatabaseEmpty("TEST");
		TestDB.ifDatabaseEmpty("ow");
		TestDB.ifDatabaseEmpty("admin");

		TestDB.print_ifExistCollection("ow","test");	    	
		TestDB.print_ifExistCollection("ow","hello");

		TestDB.client.close();
	}
	
}