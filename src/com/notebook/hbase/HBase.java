package com.notebook.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;

import com.notebook.bean.Book;


public class HBase {
	
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
	 * 获取连接
	 * @return
	 */
	public static Connection getConn(){
		return conn;
	}
	
	/**
	 * 查看HBase中的表
	 * @throws IOException
	 */
	public static void list(){
		//获取HBase管理实例对象
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			for (TableName tn: admin.listTableNames()) {
				System.out.println("表："+tn.getNameAsString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (admin!=null) {
				try {
					admin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
	}
	
	/**
	 * 创建HTable
	 * @param tableName
	 * @param familys
	 * @return
	 * @throws IOException
	 */
	public static boolean create(String tableName,String ...familys){		
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				System.out.println("表已存在");
				return false;
			}else {
				HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
				for (String family : familys) {
					HColumnDescriptor hcd = new HColumnDescriptor(family);
					htd.addFamily(hcd);
				}
				admin.createTable(htd);
				System.out.println("表创建成功");
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (admin!=null) {
				try {
					admin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}
	
	/**
	 * 删除HTable
	 * @param tableName
	 * @throws IOException
	 */
	public static void delTable(String tableName) {
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				admin.disableTable(TableName.valueOf(tableName));
				admin.deleteTable(TableName.valueOf(tableName));
				System.out.println(tableName+"表已删除");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (admin!=null) {
				try {
					admin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 获取表描述信息
	 * @param tableName
	 */
	public static void describe(String tableName){
		try {
			Admin admin = conn.getAdmin();
			HTableDescriptor htd= admin.getTableDescriptor(TableName.valueOf(tableName));
			System.out.println("===describe "+tableName+"===");
			for(HColumnDescriptor hcd:htd.getColumnFamilies()) {
				System.out.println(hcd.getNameAsString());
			}
			System.out.println("=======================");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 添加表列族
	 * @param tableName
	 * @param familys
	 * @throws IOException
	 */
	public static void addFamilys(String tableName,String ...familys) {
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
				for (String family : familys) {
					HColumnDescriptor hcd = new HColumnDescriptor(family);
					htd.addFamily(hcd);
				}
				admin.modifyTable(TableName.valueOf(tableName), htd);
				System.out.println("列族添加成功");
			}else {
				System.out.println("表不存在");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (admin!=null) {
				try {
					admin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 删除表列族
	 * @param tableName
	 * @param family
	 */
	public static void delFamily(String tableName,String family){
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
				if(htd.hasFamily(Bytes.toBytes(family))){
					htd.removeFamily(Bytes.toBytes(family));
					admin.modifyTable(TableName.valueOf(tableName), htd);
					System.out.println("列族删除成功");
				}else {
					System.out.println("列族不存在");
				}
			}else {
				System.out.println("表不存在");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (admin!=null) {
				try {
					admin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 删除列键
	 * @param tableName
	 * @param family
	 * @param columnKey
	 */
	public static void delColumnKey(String tableName,String family,String columnKey){
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
				if(htd.hasFamily(Bytes.toBytes(family))){
					HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes(family));
					hcd.remove(Bytes.toBytes(columnKey));
					System.out.println("删除成功");
				}else {
					System.out.println("列族不存在");
				}
			}else {
				System.out.println("表不存在");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 添加数据
	 * @param tableName
	 * @param key
	 * @param kvs
	 * @throws IOException
	 */
	public static void put(String tableName,String key,String[][] kvs){	
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			List<Put> lp = new ArrayList<Put>();
			for (String[] kv : kvs) {
				Put put = new Put(Bytes.toBytes(key));
				put.addColumn(Bytes.toBytes(kv[0]),
							  Bytes.toBytes(kv[1]),
							  Bytes.toBytes(kv[2]));
				lp.add(put);
			}
			table.put(lp);
			System.out.println("添加成功");
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
	 * 根据rowKey 获取books
	 * @param tableName
	 * @param rowKey
	 */
	public static List<Book> getBooks(String tableName,String user){
		Table table = null;
		List<Book> books = new ArrayList<Book>();
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			System.out.println("执行到了Dao-getBooks");
			//行键过滤器   BinaryPrefixComparator | SubstringComparator | RegexStringComparator | BinaryPrefixComparator
			RowFilter rf = new RowFilter(CompareOp.EQUAL,
					new BinaryPrefixComparator(Bytes.toBytes(user+"_")));
			scan.setFilter(rf);
			//扫描出用户对应的所有Books
			ResultScanner results = table.getScanner(scan);
			for (Result row : results) {
				for (Cell cell : row.listCells()) {
					String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
					if (qualifier.equals("nbn")) {
						String value = Bytes.toString(CellUtil.cloneValue(cell));
						String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
						books.add(new Book(user,rowKey, value));
					}
				}
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
		return books;
	}
	
	/**
	 * 遍历表内容
	 * @param tableName
	 */
	public static void scan(String tableName){
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Scan scan = new Scan();
			//全表扫描，可以指定列族、列键、不能有行健
			ResultScanner rs = table.getScanner(scan);
			System.out.println("====scan "+tableName+"=====");
			for (Result row : rs) {
				for (Cell cell : row.listCells()) {
					System.out.println("Rowkey:"+Bytes.toString(row.getRow())+"\t"
						+"Family:"+Bytes.toString(CellUtil.cloneFamily(cell))+"\t"
						+"Quilifier:"+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"
						+"Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
				}
			}
			System.out.println("=========================");
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
	 * 遍历表(带过滤器)
	 * @param tableName
	 * @param filterList
	 */
	public static void scan(String tableName,Scan scan){
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			//全表扫描，可以指定列族、列键、不能有行健
			ResultScanner rs = table.getScanner(scan);
			for (Result row : rs) {
				for (Cell cell : row.listCells()) {
					System.out.println("Rowkey:"+Bytes.toString(row.getRow())+"\t"
						+"Family:"+Bytes.toString(CellUtil.cloneFamily(cell))+"\t"
						+"Quilifier:"+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"
						+"Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
				}
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
	 * 查询条件例子
	 * @return
	 */
	public static Scan scan(){
		Scan scan = new Scan();
		//列值过滤器
		SingleColumnValueFilter scvf = 
				new SingleColumnValueFilter(Bytes.toBytes("info"),
				Bytes.toBytes("song1"), 
				CompareOp.EQUAL, 
				Bytes.toBytes("冰雨"));
		//行键过滤器   BinaryPrefixComparator | SubstringComparator | RegexStringComparator
		RowFilter rf = new RowFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("刘德华")));
//		scan.setFilter(scvf);
//		scan.setFilter(rf);	
		//过滤器链
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		filterList.addFilter(scvf);
		filterList.addFilter(rf);
		scan.setFilter(filterList);
		
//		设置查询起止rowkey
		scan.setStartRow(Bytes.toBytes("a1"));
		scan.setStopRow(Bytes.toBytes("刘德华"));
		return scan;
	}
	

	/**
	 * 表是否存在
	 * @param tableName
	 * @return
	 */
	public static boolean exist(String tableName) {
		Admin admin = null;
		try {
			admin = conn.getAdmin();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	
	/**
	 * 测试入口
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) {		
		//设置连接参数:HBase数据库所在的主机IP
//		configuration.set("hbase.zookeeper.quorum", "192.168.50.100");
		//设置连接参数:HBase数据库使用的端口
//		configuration.set("hbase.zookeeper.property.clientPort", "2181");
//		delTable("student");
//		create("namelist", "details");
//		list();
//		conn.close();
//		list();
//		get("music", "刘德华");
//		describe("music");
//		String [][] column = {{"info","song1","冰雨"}};
//		put("music", "刘德华", column);
//		scan("music");
//		Scan scan = new Scan();
//		SingleColumnValueFilter scvf = new SingleColumnValueFilter(
//				Bytes.toBytes("info"), 
//				Bytes.toBytes("name"), 
//				CompareOp.EQUAL, 
//				Bytes.toBytes("冰雨"));
//		scan.setFilter(scvf);
//		scan("music", scan);
//		describe("music");
//		scan("namelist");
//		delTable("namelist");
		list();
	}

}
