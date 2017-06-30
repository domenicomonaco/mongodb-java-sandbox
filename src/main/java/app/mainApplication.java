package app;

import mongodb.MongoDbConnector;

public class mainApplication {

	public mainApplication() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

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
