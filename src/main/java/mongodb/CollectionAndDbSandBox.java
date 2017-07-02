package mongodb;

import java.net.UnknownHostException;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.util.JSON;

public class CollectionAndDbSandBox {
	

	private String dbname;
	private MongoDatabase db;
	private MongoDbConnector connect;


	/**
	 * Constructor for DataWrapper, set mongoclient and 
	 * @param cl
	 * @param db
	 */
	public CollectionAndDbSandBox(MongoDbConnector connect, String dbname) {
		this.connect = connect;
		this.dbname = dbname;	
	}
	
	boolean checkDB() {
		if(this.connect.ifExistDB(this.dbname)) {
			this.db = connect.getClient().getDatabase(this.dbname);
			System.out.println("# OK: Database " + this.dbname +  " exist" );
			return true;
		}else if(this.connect.ifExistDifferentCaseDB(this.dbname)){
			System.out.println("# Error: Database " + this.dbname +  " exist but have different CASE (lower or upper), this is list:" );
			this.connect.print_listDatabase();
			return false;
		}else{
			System.out.println("# Error: Database " + this.dbname +  " do not exist, this is list:" );
			this.connect.print_listDatabase();
			return false;
		}
	}

	
	public MongoClient getClient() {
		return this.connect.getClient();
	}
	
	void createCollectionAndInsert_1(String collectionName) {
		
		MongoCollection<Document> collection = this.db.getCollection(collectionName);
		
		int i=0;
		for(i=0;i<=10;i++) {
			Document document = new Document();
			document.put("name", "mkyong");
			document.put("age", 30);
			document.put("createdDate","s");
			document.put("diue","s");
			document.append("info", (BasicDBObject) JSON.parse("{'name':'io', 'age':21}"));
			
			//System.out.println(i);
			collection.insertOne(document);
		}
		
	}
	
	MongoCollection<Document> createCollectionCappedSize(String collectionName, long size) {
		
		if(this.connect.ifExistCollection(this.dbname, collectionName)==false){
		this.db.createCollection(collectionName,
		          new CreateCollectionOptions().capped(true).sizeInBytes(size));

		System.out.println("# OK: Collcection " + collectionName + " was created with capped size "+ size);
		return this.db.getCollection(collectionName);
		}else{

			System.out.println("# ERROR: Collcection " + collectionName + " was not, exist another with same name");
			return null;
		}
		
		
	}
	
	public static void main(final String[] args) throws UnknownHostException {

		CollectionAndDbSandBox data = new CollectionAndDbSandBox(new MongoDbConnector(),"Hello");
		
		
		if(data.checkDB()) {
			data.createCollectionAndInsert_1("ciaone");
			data.createCollectionCappedSize("red", 0x100000);
		}
		
		
		/**
		//EXAMPLE 'vincol'
		ValidationOptions collOptions = 
				new ValidationOptions().validator(
		        Filters.or(Filters.exists("email"),
		        		Filters.exists("phone")));
		
		TestDB.client.getDatabase("24").createCollection("contacts",
		        new CreateCollectionOptions().validationOptions(collOptions));
		
		table = TestDB.client.getDatabase("24").getCollection("contacts");
		i=0;
		for(i=0;i<=10;i++) {
			Document document = new Document();
			document.put("email", "mkyong");
			document.put("phone", 30);
			document.put("createdDate","s");
			document.put("diue","s");
			document.append("info", (BasicDBObject) JSON.parse("{'name':'io', 'age':21}"));
			
			System.out.println(i);
			table.insertOne(document);
		}
		
		i=0;
		for(i=0;i<=10;i++) {
			Document document = new Document();

			document.put("phone", 30);
			document.put("createdDate","s");
			document.put("diue","s");
			document.append("info", (BasicDBObject) JSON.parse("{'name':'io', 'age':21}"));
			
			System.out.println(i);
			table.insertOne(document);
		}**/
	}
	
}
