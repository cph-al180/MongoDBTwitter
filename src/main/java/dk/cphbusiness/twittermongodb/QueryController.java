/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dk.cphbusiness.twittermongodb;

import com.mongodb.client.AggregateIterable;
import org.bson.Document;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Andreas
 */
public class QueryController {

    private MongoConnection mongo;

    public QueryController(MongoConnection mongo) {
        this.mongo = mongo;
    }

    public int GetNumberOfUsers() {
        int count = 0;
        AggregateIterable<Document> output = mongo.GetCollection().aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$user")),
                new Document("$group", new Document("_id", null).append("count", new Document("$sum", 1))),
                new Document("$project", new Document("_id", 0).append("count", 1))
        ));
        for (Document doc : output) {
            count = (int) doc.get("count");
        }
        return count;
    }

    public List<Object> GetTopTenMostActiveUsers() {
        List<Object> TopTenMostActiveUsers = new ArrayList<>();
        AggregateIterable<Document> output = mongo.GetCollection().aggregate(Arrays.asList(
                new Document("$group", new Document("_id", new Document("user", "$user").append("tweet_id", "$id"))),
                new Document("$group", new Document("_id", "$_id.user").append("tweet_count", new Document("$sum", 1))),
                new Document("$project", new Document("_id", 0).append("user", "$_id").append("tweet_count", 1)),
                new Document("$sort", new Document("tweet_count", -1)),
                new Document("$limit", 10)
        )).allowDiskUse(Boolean.TRUE);
        for (Document doc : output) {
            TopTenMostActiveUsers.add(doc.get("user") + ", " + doc.get("tweet_count"));
        }
        return TopTenMostActiveUsers;
    }

    public List<Object> GetTopTenLinkingUsers() {
        List<Object> TopTenLinkingUsers = new ArrayList<>();
        AggregateIterable<Document> output = mongo.GetCollection().aggregate(Arrays.asList(
                new Document("$match", new Document("text", new Document("$regex", ".*@.*"))),
                //{$match: {text: regex}},
                new Document("$group", new Document("_id", new Document("user", "$user").append("tweet_id", "$id"))),
                new Document("$group", new Document("_id", "$_id.user").append("tweet_count", new Document("$sum", 1))),
                new Document("$project", new Document("_id", 0).append("user", "$_id").append("tweet_count", 1)),
                new Document("$sort", new Document("tweet_count", -1)),
                new Document("$limit", 10)
        )).allowDiskUse(Boolean.TRUE);
        for (Document doc : output) {
            TopTenLinkingUsers.add(doc.get("user") + ", " + doc.get("tweet_count"));
        }
        return TopTenLinkingUsers;
    }

    public List<Object> getGrumpiestUsers() {
        List<Object> condArray = new ArrayList<>();
        List<Object> eqArray = new ArrayList<>();
        List<Object> divideArray = new ArrayList<>();
        eqArray.add("$polarity");
        eqArray.add(0);
        divideArray.add("$tweet_count");
        divideArray.add("$polarity");
        condArray.add(new Document("$eq", eqArray));
        condArray.add(0);
        condArray.add(new Document("$divide", divideArray));
        AggregateIterable<Document> output = mongo.GetCollection().aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$user")
                        .append("polarity", new Document("$sum", "$polarity"))
                        .append("tweet_count", new Document("$sum", 1))),
                new Document("$project", new Document("_id", 0)
                        .append("user", "$_id")
                        .append("avg_polarity", new Document("$cond", condArray))
                        .append("polarity", 1)
                        .append("tweet_count", 1)),
                new Document("$sort", new Document("avg_polarity", 1)
                        .append("tweet_count", -1)),
                new Document("$limit", 5)
        )).allowDiskUse(Boolean.TRUE);
        List<Object> grumpiestUsers = new ArrayList<>();
        for (Document dbObject : output) {
            System.out.println(dbObject);
            grumpiestUsers.add(dbObject.get("user") + ", " + dbObject.get("tweet_count") + ", " + dbObject.get("polarity"));
        }
        return grumpiestUsers;
    }

    public List<Object> getHappiestUsers() {
        List<Object> condArray = new ArrayList<>();
        List<Object> eqArray = new ArrayList<>();
        List<Object> divideArray = new ArrayList<>();
        eqArray.add("$polarity");
        eqArray.add(0);
        divideArray.add("$tweet_count");
        divideArray.add("$polarity");
        condArray.add(new Document("$eq", eqArray));
        condArray.add(0);
        condArray.add(new Document("$divide", divideArray));
        AggregateIterable<Document> output = mongo.GetCollection().aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$user")
                        .append("polarity", new Document("$sum", "$polarity"))
                        .append("tweet_count", new Document("$sum", 1))),
                new Document("$project", new Document("_id", 0)
                        .append("avg_polarity", new Document("$cond", condArray))
                        .append("polarity", 1)
                        .append("tweet_count", 1)),
                new Document("$sort", new Document("avg_polarity", -1)
                        .append("tweet_count", -1)),
                new Document("$limit", 5)
        )).allowDiskUse(Boolean.TRUE);
        List<Object> happiestUsers = new ArrayList<>();
        for (Document dbObject : output) {
            System.out.println(dbObject);
            happiestUsers.add(dbObject.get("user") + ", " + dbObject.get("tweet_count") + ", " + dbObject.get("polarity"));
        }
        return happiestUsers;
    }
}
