/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dk.cphbusiness.twittermongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 *
 * @author Andreas
 */
public class MongoConnection {
    
    private String clientURI;
    private String dbName;
    private String collectionName;
    private MongoCollection<Document> collection;
    
    public MongoConnection(String clientURI, String dbName, String collectionName){
        this.clientURI = clientURI;
        this.dbName = dbName;
        this.collectionName = collectionName;
        MongoClientURI con = new MongoClientURI(clientURI);
        MongoClient client = new MongoClient(con);
        MongoDatabase db = client.getDatabase(dbName);
        this.collection = db.getCollection(collectionName);
    }
    
    public MongoCollection<Document> GetCollection(){
        return this.collection;
    }
    
}
