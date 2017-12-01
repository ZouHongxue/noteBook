package com.notebook.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.springframework.stereotype.Service;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.notebook.bean.Book;
import com.notebook.dao.NoteDao;
import com.notebook.hbase.HBase;
import com.notebook.util.RedisUtils;

import redis.clients.jedis.Jedis;

@Service
public class NoteService {
	
	private final String tableName = "nb";
	private final String notetable = "n";
	
	public String getNotes(String rowKey){
		String notes = null;
		if (!HBase.exist(tableName)) {
			return notes;
		}
		notes = NoteDao.getNotes(tableName, rowKey);
		return notes;
	}

	public boolean addNote(String rowKey,String note){
		if (!HBase.exist(tableName)) {
			return false;
		}
		String notes = NoteDao.getNotes(tableName, rowKey);
		if (notes.equals(" ")) {
			String[][] kvs = {{"nbi","nl",note}};
			HBase.put(tableName, rowKey, kvs);
		}else {
			String[][] kvs = {{"nbi","nl",notes+","+note}};
			HBase.put(tableName, rowKey, kvs);
		}
		return true;
	}

	
	/**
	 * 保存note内容，检查是否已有note,content作为列键
	 * @param user
	 * @param note
	 * @param text
	 * @return
	 */
	public boolean saveNote(String user, String note,String text){
		if (!HBase.exist(notetable)) {
			HBase.create(notetable, "ni");
			System.out.println(notetable+"表创建成功");
		}
		String rowKey = null;
		rowKey = NoteDao.isNoteExist(notetable,user,note);
		if (rowKey!=null){
			String[][] kvs = {{"ni","content",text}};
			HBase.put(notetable, rowKey, kvs);
			return true;
		}else {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			rowKey = user+"_"+note+"_"+sdf.format(new Date());
			String[][] kvs = {{"ni","content",text}};
			HBase.put(notetable, rowKey, kvs);
			return true;
		}
	}
	
	public String getNoteText(String user,String note){
		String text = " ";
		if (!HBase.exist(notetable)) {
			return text;
		}
		text = NoteDao.getNoteText(notetable, user, note);
		return text;
	}
	
}
