/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dk.cphbusiness.twittermongodb;

import java.util.List;
/**
 *
 * @author Andreas
 */
public class QueryRunner {

    public static void main(String[] args) {
        MongoConnection mongo = new MongoConnection("mongodb://localhost:27017", "social_net", "tweets");
        QueryController qc = new QueryController((mongo));
        System.out.println("\nNumber of accounts\n" + qc.GetNumberOfUsers());
        System.out.println("\nTop 10 most active users");
        PrintLineList(qc.GetTopTenMostActiveUsers());
        System.out.println("\nTop 10 most linking users");
        PrintLineList(qc.GetTopTenLinkingUsers());
    }
    
    private static void PrintLineList(List<Object> list){
       List<Object> oList = list;
       int count = 1;
       for(Object obj : oList){
           System.out.println(count++ + ": " + obj.toString());
       }
    }
}
