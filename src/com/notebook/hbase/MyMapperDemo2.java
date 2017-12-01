package com.notebook.hbase;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapperDemo2 {
	
	static class MySonNameMapper extends TableMapper<Text, Put> {
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text, Put>.Context context)
				throws IOException, InterruptedException {
			List<Cell> cells = value.listCells();
			for (Cell cell : cells) {
				String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
				String kv = Bytes.toString(CellUtil.cloneValue(cell));
			
				//if(qualifier.equals("name")) {
					Put put = new Put(CellUtil.cloneValue(cell));
					put.addColumn("namelist".getBytes(),"num".getBytes(), "0".getBytes());
					context.write(new Text(Bytes.toString(CellUtil.cloneValue(cell))), put);
				//}
				//context.write(new Text(CellUtil.getCellKeyAsString(cell)), new Text(str));
			}
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		File jarFile = EJob.createTempJar("bin");
		ClassLoader classLoader = EJob.getClassLoader();
		Thread.currentThread().setContextClassLoader(classLoader);
		System.setProperty("HADOOP_USER_NAME", "zouhongxue");   
		
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf);
		((JobConf) job.getConfiguration()).setJar(jarFile.toString());
		
		job.setJarByClass(MyMapperDemo2.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "music2");
		Scan scan = new Scan();
		scan.addColumn("info".getBytes(), "name".getBytes());
		
		TableMapReduceUtil.initTableMapperJob(
				"music",
				scan, 
				MySonNameMapper.class,
				Text.class,
				Put.class, 
				job);
		
		job.waitForCompletion(true);

	}

}
