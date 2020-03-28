package com.bsoft.test;

import com.bsoft.util.MongoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class MogoDBTest {


    //插入一个文档
    @Test
    public void insertOneTest(){
    	
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoUtil.getDB("books");
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection("book");
        //要插入的数据
        Document document = new Document("id",1)
                .append("name", "哈姆雷特")
                .append("price", 67);
        //插入一个文档
        collection.insertOne(document);
        System.out.println(document.get("_id"));
    }

    //插入多个文档
    @Test
    public void insertManyTest(){
        //获取数据库连接对象
        MongoDatabase mongoDatabase = MongoUtil.getDB("books");
        //获取集合
        MongoCollection<Document> collection = mongoDatabase.getCollection("book");
        //要插入的数据
        List<Document> list = new ArrayList<>();
        for(int i = 1; i <= 5; i++) {
            Document document = new Document("id",i)
                    .append("name", "book"+i)
                    .append("price", 20+i);
            list.add(document);
        }
        //插入多个文档
        collection.insertMany(list);
    }


    @Test
    public void findAllTest(){
        //连接到数据库
        MongoDatabase mongoDatabase =  MongoUtil.getDB("books");
        //获取集合
        MongoCollection<Document> mongoCollection= mongoDatabase.getCollection("book");
        //查询集合的所有文档
        FindIterable findIterable= mongoCollection.find().sort(new BasicDBObject("price",1));
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        MongoUtil.close();
    }


    //指定查询过滤器查询
    @Test
    public void FilterfindTest(){
        //连接到数据库
        MongoDatabase mongoDatabase =  MongoUtil.getDB("books");
        //获取集合
        MongoCollection<Document> collection= mongoDatabase.getCollection("book");
        //指定查询过滤器
        Bson filter = Filters.eq("id", 2);
        //指定查询过滤器查询
        FindIterable findIterable = collection.find(filter);
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    //取出查询到的第一个文档
    @Test
    public void findTest() {
        //连接到数据库
        MongoDatabase mongoDatabase =  MongoUtil.getDB("books");
        //获取集合
        MongoCollection<Document> collection= mongoDatabase.getCollection("book");
        //查找集合中的所有文档
        FindIterable findIterable = collection.find();
        //取出查询到的第一个文档
        Document document = (Document) findIterable.first();
        //打印输出
        System.out.println(document);
    }

    //修改文档
    @Test
    public void updateTest(){
        //连接到数据库
        MongoDatabase mongoDatabase =  MongoUtil.getDB("books");
        //获取集合
        MongoCollection<Document> collection= mongoDatabase.getCollection("book");
        //修改过滤器
        Bson filter = Filters.eq("id", 2);
        //指定修改的更新文档
        Document document = new Document("$set", new Document("price", 33));
        //修改单个文档
       // collection.updateOne(filter, document);
        //修改多个文档
       collection.updateMany(filter, document);
    }

    //删除与筛选器匹配的单个文档
    @Test
    public void deleteOneTest(){
        //连接到数据库
        MongoDatabase mongoDatabase =  MongoUtil.getDB("books");
        //获取集合
        MongoCollection<Document> collection= mongoDatabase.getCollection("book");
        //申明删除条件
        Bson filter = Filters.eq("id",3);
        //删除与筛选器匹配的单个文档
        collection.deleteOne(filter);
    }

    //删除与筛选器匹配的所有文档
    @Test
    public void deleteManyTest(){
        //连接到数据库
        MongoDatabase mongoDatabase =  MongoUtil.getDB("books");
        //获取集合
        MongoCollection<Document> collection= mongoDatabase.getCollection("book");
        //申明删除条件
        Bson filter = Filters.eq("id",4);
        //删除与筛选器匹配的所有文档
        collection.deleteMany(filter);
    }

    @Test
    public void deleteAllTest(){
        //连接到数据库
        MongoDatabase mongoDatabase =  MongoUtil.getDB("books");
        //获取集合
        MongoCollection<Document> collection= mongoDatabase.getCollection("book");
        //删除与筛选器匹配的所有文档
        collection.deleteMany(new Document());
    }

}
