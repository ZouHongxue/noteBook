package com.notebook.hbase;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

public class HDFSUtils {
	/**
	 * 上传本地文件
	 * @param src
	 * @param dst
	 * @throws IOException 
	 * @throws URISyntaxException 
	 */
	public static void uploadFile(String src,String dst) throws IOException, URISyntaxException{
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://zhx01:9000"),conf);
		Path fsrc = new Path(src);
		Path fdst = new Path(dst);
		fs.copyFromLocalFile(fsrc, fdst);
		System.out.println("Upload to "+conf.get("fs.defaultFS"));
		FileStatus[] files = fs.listStatus(fdst);
		for(FileStatus file:files){
			System.out.println(file.getPath());
		}
		fs.close();
	}
	/**
	 * 创建文件
	 * @param dst
	 * @param contents
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void createFile(String dst ,byte[] contents) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf);
		Path fdst =new Path(dst);
		FSDataOutputStream outputStream = fs.create(fdst);
		outputStream.write(contents);
		outputStream.close();
		System.out.println("文件创建成功！");
		fs.close();
	}
	
	/**
	 * 文件重命名
	 * @param oldName
	 * @param newName
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void rename (String oldName,String newName) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf);
		Path oldPath =new Path(oldName);
		Path newPath = new Path(newName);
		boolean ok = fs.rename(oldPath, newPath);
		if(ok){
			System.out.println("重命名成功！");
		}else{
			System.out.println("重命名失败！");
		}
		fs.close();
	} 
	/**
	 * 删除文件
	 * @param filePath
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void delete(String filePath) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf);
		Path path = new Path(filePath);
		boolean ok = fs.deleteOnExit(path);
		if(ok){
			System.out.println("删除成功！");
		}else{
			System.out.println("删除失败！");
		}
		fs.close();
	}
	
	/**
	 * 创建目录
	 * @param path
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void mkdir(String path) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf);
		Path srcPath = new Path(path);
		boolean ok = fs.mkdirs(srcPath);
		if(ok){
			System.out.println("创建成功！");
		}else{
			System.out.println("创建失败！");
		}
	}
	/**
	 * 读取文件的内容
	 * @param filePath
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void readFile(String filePath) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(new URI("hdfs://zhx01:9000"),conf);
		Path srcPath = new Path(filePath);
		InputStream in = null;
		
		in = fs.open(srcPath);
		IOUtils.copyBytes(in, System.out, 4096, false);
		IOUtils.closeStream(in);
		fs.close();
	}
	

	/**
	 * 读取分区文件的内容
	 * @param filePath
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void readFiles(String filePath,int i) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf);
		Path srcPath = null;
		InputStream in = null;
		for (int j = 0; j < i; j++) {
			System.out.println("分区"+j);
			srcPath = new Path(filePath.substring(0, filePath.length()-1).concat(String.valueOf(i)));
			in = fs.open(srcPath);
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
		IOUtils.closeStream(in);
		fs.close();
	}
	
	/**
	 * 检测文件是否存在
	 * @param dst
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void exists(String dst) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf);
		Path fdst = new Path(dst);
		boolean ok = fs.exists(fdst);
		System.out.println(ok?"文件存在":"文件不存在");
		fs.close();
	}
	
	/**
	 * 获取节点信息
	 * @throws IOException
	 */
	public static void getHostName() throws IOException{
		Configuration conf = new Configuration();
		System.setProperty("HADOOP_USER_NAME","hadoop");
		DistributedFileSystem fs = (DistributedFileSystem)FileSystem.get(conf);
		DatanodeInfo[] dataNodeStates = fs.getDataNodeStats();
		for(DatanodeInfo dataNode:dataNodeStates){
			System.out.println(dataNode.getHostName()+"\t"+dataNode.getName());
		}
		fs.close();
	}
	/**
	 * 获取某个文件在HDFS集群的位置
	 * @param dst
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void getFileBlockLocation(String dst) throws IOException, URISyntaxException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf);
		Path fdst = new Path(dst);
		FileStatus fileStatus = fs.getFileLinkStatus(fdst);
		BlockLocation[] blockLocation = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		for(BlockLocation bl : blockLocation){
			System.out.println(Arrays.toString(bl.getHosts())+"\t"+Arrays.toString(bl.getNames()));
		}
		fs.close();
	}
	
	/**
	 * 遍历文件
	 * @param folder
	 * @throws IOException
	 */
	public static void ls(String folder) throws IOException{
		Configuration conf = new Configuration();
		Path path = new Path(folder);
		FileSystem fs =  FileSystem.get(conf);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls:"+folder);
		System.out.println("===========================");
		for(FileStatus f : list){
			System.out.printf("name:%s,folder:%s,size:%d\n",f.getPath(),f.isDirectory(),f.getLen());
		}
		System.out.println("===========================");
		fs.close();
	}
	/**
	 * 下载文件
	 * @param remote
	 * @param local
	 * @throws IOException
	 * @throws URISyntaxException 
	 */
	public static void download(String remote,String local) throws IOException, URISyntaxException{
		Path path = new Path(remote);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf);
		fs.copyToLocalFile(path, new Path(local));
		fs.close();
	}
}
