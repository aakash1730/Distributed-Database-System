package com.aakash.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class equijoin {

	public static void main(String[] args) throws IOException {

		JobClient worker = new JobClient();
		JobConf job_conf = new JobConf(equijoin.class);

		job_conf.setJobName("equiJoin");

		job_conf.setMapperClass(KeyMapper.class);
		job_conf.setReducerClass(KeyReducer.class);
		job_conf.setOutputKeyClass(Text.class);
		job_conf.setOutputValueClass(Text.class);

		job_conf.setInputFormat(TextInputFormat.class);
		job_conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

		worker.setConf(job_conf);
		try {
			JobClient.runJob(job_conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static class KeyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] words = line.split(",");
			String joinColumn = words[1];
			String values = "";
			for (int i = 0; i < words.length; i++) {
				if (i == 0) {
					values = words[i];
					continue;
				}
				values += "," + words[i];
			}

			Text mapperKey = new Text();
			Text mapperValue = new Text();

			mapperKey.set(joinColumn);
			mapperValue.set(values);
			output.collect(mapperKey, mapperValue);

		}
	}

	public static class KeyReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			Map<String, List<String>> map = new HashMap<>();
			while (values.hasNext()) {
				String value = values.next().toString();
				String[] columnValues = value.split(",");

				if (map.containsKey(columnValues[0])) {
					List<String> li = map.get(columnValues[0]);
					li.add(value);
					map.put(columnValues[0], li);
				} else {
					List<String> li = new ArrayList<>();
					li.add(value);
					map.put(columnValues[0], li);
				}
			}

			if(map.size() <= 1) {
				key.clear();
			}
			else {
				Text finalOutputKey = new Text();
				Text finalOutputValue = new Text();
				String outputValue = null;
				Set<String> keySet = map.keySet();
				List<String> finalAnswer = new ArrayList<>();
				
				for(String mapKeys : keySet) {
					if(finalAnswer.isEmpty()) {
						finalAnswer = map.get(mapKeys);
					}
					else {
						finalAnswer = createNewJoinResultList(finalAnswer, map.get(mapKeys));
					}
				}
				
				for(String ans : finalAnswer) {
					outputValue = ans;
					finalOutputValue.set("");
					finalOutputKey.set(outputValue);
					output.collect(finalOutputKey, finalOutputValue);
				}
				
			}

		}
	}
	
	public static List<String> createNewJoinResultList(List<String> firstTableValues, List<String> secondTableValues){
		List<String> joinTuple = new ArrayList<>();
		
		for(String tupleOfFirstTable : firstTableValues) {
			for(String tupleOfSecondTable : secondTableValues) {
				joinTuple.add(tupleOfFirstTable + ", " + tupleOfSecondTable);
			}
		}
			
		return joinTuple;
	}
}
