package com.bsoft.test;

import com.bsoft.util.MongoDBUtil;
import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.BSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class MogoDBTest {


    private static MongoClient mongoClient;

    static {
        System.out.println("===============MongoDBUtil初始化========================");
        mongoClient = new MongoClient("127.0.0.1", 27017);
        // 大部分用户使用mongodb都在安全内网下，但如果将mongodb设为安全验证模式，就需要在客户端提供用户名和密码：
        // boolean auth = db.authenticate(myUserName, myPassword);
        MongoClientOptions.Builder options = new MongoClientOptions.Builder();
        options.cursorFinalizerEnabled(true);
        // options.autoConnectRetry(true);// 自动重连true
        // options.maxAutoConnectRetryTime(10); // the maximum auto connect retry time
        options.connectionsPerHost(300);// 连接池设置为300个连接,默认为100
        options.connectTimeout(30000);// 连接超时，推荐>3000毫秒
        options.maxWaitTime(5000); //
        options.socketTimeout(0);// 套接字超时时间，0无限制
        options.threadsAllowedToBlockForConnectionMultiplier(5000);// 线程队列数，如果连接线程排满了队列就会抛出“Out of semaphores to get db”错误。
        options.writeConcern(WriteConcern.SAFE);//
        options.build();
    }

    // =================公用用方法=================
    /**
     * 获取DB实例 - 指定数据库，若不存在则创建
     * @param dbName
     * @return
     */
    public static MongoDatabase getDB(String dbName) {
        if (dbName != null && !"".equals(dbName)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            return database;
        }
        return null;
    }

    /**
     * 获取指定数据库下的collection对象
     * @param collName
     * @return
     */
    public static  MongoCollection<Document> getCollection(String dbName, String collName) {
        if (null == collName || "".equals(collName)) {
            return null;
        }
        if (null == dbName || "".equals(dbName)) {
            return null;
        }
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collName);
        return collection;
    }

    //获取所有数据库
    @Test
    public void getAllDBNames(){
        MongoIterable<String> dbNames = mongoClient.listDatabaseNames();
        for (String s : dbNames) {
            System.out.println(s);
        }
    }

    //获取指定库的所有集合名
    @Test
    public void getAllCollections(){
        MongoIterable<String> colls = getDB("books").listCollectionNames();
        for (String s : colls) {
            System.out.println(s);
        }
    }

    //删除数据库
    @Test
    public void dropDB(){
        //连接到数据库
        MongoDatabase mongoDatabase =  getDB("test");
        mongoDatabase.drop();
    }

    //插入一个文档
    @Test
    public void insertOneTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
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
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //要插入的数据
        List<Document> list = new ArrayList<>();
        for(int i = 1; i <= 15; i++) {
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
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //查询集合的所有文
        FindIterable findIterable= collection.find().sort(new BasicDBObject("price",1));
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    @Test
    public void findConditionTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //方法1.构建BasicDBObject  查询条件 id大于2，小于5
        BasicDBObject queryCondition=new BasicDBObject();
        queryCondition.put("id", new BasicDBObject("$gt", 2));
        queryCondition.put("id", new BasicDBObject("$lt", 5));
        //查询集合的所有文  通过price升序排序
        FindIterable findIterable= collection.find(queryCondition).sort(new BasicDBObject("price",1));

        //方法2.通过过滤器Filters，Filters提供了一系列查询条件的静态方法   id大于2小于5   通过id升序排序查询
        //Bson filter=Filters.and(Filters.gt("id", 2),Filters.lt("id", 5));
        //FindIterable findIterable= collection.find(filter).sort(Sorts.orderBy(Sorts.ascending("id")));

        //查询集合的所有文
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    @Test
    public void findAllTest2(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        BasicDBObject queryCondition = new BasicDBObject();
        queryCondition.put("id", new BasicDBObject("$gt", 2));
        queryCondition.put("id", new BasicDBObject("$lt", 5));

        Filters.and(Filters.gt("id", 2),Filters.lt("id", 3));
        //查询集合的所有文档
        FindIterable findIterable= collection.find(queryCondition).sort(new BasicDBObject("price",1));
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        MongoDBUtil.close();
    }

    @Test
    public void findAllTest3(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        Bson fileter=Filters.in("id",1,2,3,4);
        //查询集合的所有文档
        FindIterable findIterable= collection.find(fileter).projection(new BasicDBObject("id",1).append("name",1).append("_id",0));
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    //分页查询
    @Test
    public void findByPageTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //分页查询
        FindIterable findIterable= collection.find().skip(0).limit(10).sort(Sorts.orderBy(Sorts.ascending("id")));
        MongoCursor cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("----------取出查询到的第一个文档-----------------");
        //取出查询到的第一个文档
        Document document = (Document) findIterable.first();
        //打印输出
        System.out.println(document);
    }

    //修改文档
    @Test
    public void updateTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //修改过滤器
        Bson filter = Filters.eq("id", 2);
        //指定修改的更新文档
        Document document = new Document("$set", new Document("price", 44));
        //修改单个文档
       // collection.updateOne(filter, document);
        //修改多个文档
      // collection.updateMany(filter, document);

        //修改全部文档
        collection.updateMany(new BasicDBObject(),document);
    }

    //删除与筛选器匹配的单个文档
    @Test
    public void deleteOneTest(){
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //申明删除条件
        Bson filter = Filters.eq("id",3);
        //删除与筛选器匹配的单个文档
        collection.deleteOne(filter);

        //删除与筛选器匹配的所有文档
       // collection.deleteMany(filter);

        System.out.println("--------删除所有文档----------");
        //删除与筛选器匹配的所有文档
        collection.deleteMany(new Document());
    }

    //集合的文档数统计
    @Test
    public void getCountTest() {
        //获取集合
        MongoCollection<Document> collection = getCollection("books","book");
        //获取集合的文档数
        Bson filter = Filters.gt("price", 30);
        int count = (int)collection.count(filter);
        System.out.println("价钱大于30的count==："+count);
    }

}


