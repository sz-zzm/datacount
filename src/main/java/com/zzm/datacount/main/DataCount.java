package com.zzm.datacount.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
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
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		job.setPartitionerClass(ProvidePartitioner.class);
		
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
	
	public static class ProvidePartitioner extends Partitioner<Text, DataBean> {
		
		private static Map<String, Integer> provideMap = new HashMap<String, Integer>();
		
		/**
		 * 预加载数据
		 */
		static {
			provideMap.put("135", 1);
			provideMap.put("136", 2);
			provideMap.put("137", 3);
			provideMap.put("138", 4);
		}
		
		@Override
		public int getPartition(Text key, DataBean value, int numPartitions) {
			// TODO Auto-generated method stub
			String account = key.toString();
			String sub_acc = account.substring(0, 3);
			Integer code = provideMap.get(sub_acc);
			if (code == null) {
				code = 0;
			}
			return code;
		}
		
	}
}
