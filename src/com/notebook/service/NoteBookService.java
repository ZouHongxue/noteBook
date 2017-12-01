package com.notebook.service;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.stereotype.Service;

import com.notebook.bean.Book;
import com.notebook.hbase.HBase;
import com.notebook.util.RedisUtils;

import redis.clients.jedis.Jedis;

@Service
public class NoteBookService {
	
	private final String tableName = "nb";
	public boolean addBook(String user,String book){
		Jedis jedis = RedisUtils.getJedis();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String rowKey = user+"_"+sdf.format(new Date());
		if (!HBase.exist(tableName)) {
			HBase.create("nb", "nbi");
		}
//		HBase.list();
		String[][] kvs = {{"nbi","nbn",book}};
		HBase.put(tableName, rowKey, kvs);
		HBase.scan(tableName);
		jedis.lpush(user, rowKey+","+book);
		return true;
	}
	
	public List<Book> getAll(String user){
		Jedis jedis = RedisUtils.getJedis();
		List<String> list = new ArrayList<String>();
		List<Book> books = new ArrayList<Book>();
		
		if (jedis.exists(user)) {
			list = RedisUtils.lget(user);
			System.out.println("======执行Redis========");
			for (String string : list) {
				System.out.println(string);
				books.add(new Book(user,string.split(",")[0], string.split(",")[1]));
			}
		}else {
			System.out.println("======执行HBase========");
			if(HBase.exist(tableName)){
				books = HBase.getBooks(tableName, user);
				for (Book book : books) {
					jedis.lpush(user, book.getRowKey()+","+book.getBook());
				}
			}
		}
		return books;
	}
	
	
	public void test(){
		HBase.scan(tableName);
	}
}
