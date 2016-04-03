package com.zzm.datacount.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zzm.datacount.domain.DataBean;

public class DataCount {

	/**
	 * 入口函数
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(DataCount.class);
		job.setMapperClass(DCMapper.class);
		/**
		 * setMapOutputKeyClass, setMapOutputValueClass在满足一定条件的情况下可以省略
		 * （k2 v2 与 k2 v3 类型一样时可以省略）
		 */
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DataBean.class);
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path("/input/data.txt"));
		
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path("/output/res2"));
		
		job.waitForCompletion(true);
		
	}
	
	public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean> {

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DataBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString(); 
			String[] fields = line.split("\t");
			String telNo = fields[0];
			Long upPayLoad = Long.parseLong(fields[1]);
			Long downPayLoad = Long.parseLong(fields[2]);
			DataBean dataBean = new DataBean(telNo, upPayLoad, downPayLoad);
			context.write(new Text(telNo), dataBean);
		}
		
	}
	
	public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean> {

		@Override
		protected void reduce(Text key, Iterable<DataBean> v2s, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			long up_sum = 0;//上行总流量
			long down_sum = 0;//下行总流量
			for (DataBean bean : v2s) {
				up_sum += bean.getUpPayLoad();
				down_sum += bean.getDownPayLoad();
			}
			DataBean dataBean = new DataBean("", up_sum, down_sum);
			context.write(key, dataBean);
		}
		 
	}
}
