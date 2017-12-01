package com.notebook.dao;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Component;

@Component
public class NoteDao {
	
	private static Connection conn = null;
	static{
		//创建配置对象
		Configuration configuration = HBaseConfiguration.create();
		//创建Hbase连接
		try {
			conn = ConnectionFactory.createConnection(configuration);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据rowKey 获取列键值
	 * @param tableName
	 * @param rowKey
	 */
	public static void get(String tableName,String rowKey){
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = table.get(get);
			for (Cell cell : result.listCells()) {
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				System.out.println(family+"\t"+qualifier+"\t"+value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (table!=null) {
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 获得note列表
	 * @param tableName
	 * @param rowKey
	 * @return
	 */
	public static String getNotes(String tableName,String rowKey) {
		Table table = null;
		String res = " ";
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			Result result = table.get(get);
			for (Cell cell : result.listCells()) {
				String family = Bytes.toString(CellUtil.cloneFamily(cell));
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String value = Bytes.toString(CellUtil.cloneValue(cell));
				if (qualifier.equals("nl")) {
					res = value;
				}
				System.out.println(family+"\t"+qualifier+"\t"+value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return res;
	}

	public static String isNoteExist(String notetable,String user,String note) {
		Table table = null;
		String res = null;
		try {
			table = conn.getTable(TableName.valueOf(notetable));
			Scan scan = new Scan();
			RowFilter rf = new RowFilter(CompareOp.EQUAL, 
					new BinaryPrefixComparator(Bytes.toBytes(user+"_"+note+"_")));
			scan.setFilter(rf);
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				for (Cell cell : result.listCells()) {
					String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
					String family = Bytes.toString(CellUtil.cloneFamily(cell));
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					res = rowKey;
					System.out.println(family+"\t"+qualifier+"\t"+value);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return res;
	}
	
	public static String getNoteText(String notetable,String user,String note) {
		Table table = null;
		String res = null;
		try {
			table = conn.getTable(TableName.valueOf(notetable));
			Scan scan = new Scan();
			RowFilter rf = new RowFilter(CompareOp.EQUAL, 
					new BinaryPrefixComparator(Bytes.toBytes(user+"_"+note+"_")));
			scan.setFilter(rf);
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				for (Cell cell : result.listCells()) {
					String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
					String family = Bytes.toString(CellUtil.cloneFamily(cell));
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					String value = Bytes.toString(CellUtil.cloneValue(cell));
					res = value;
					System.out.println(rowKey+"\t"+family+"\t"+qualifier+"\t"+value);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return res;
	}
}
