package Team7;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RF_Driver {
public static int support = 0;
public static int confidence = 0;
public static ArrayList<String> attributes = new ArrayList<String>();
public static ArrayList<String> dataset = new ArrayList<String>();
public static ArrayList<String> atr_List = new ArrayList<String>();
public static ArrayList<String> stableattributes = new ArrayList<String>();
public static ArrayList<String> decisionattributes = new ArrayList<String>();
public static ArrayList<String> dec_List = new ArrayList<String>();
public static String dec_from = new String();
public static String dec_to = new String();
public static Scanner scanner = new Scanner(System.in);
public static int index = 0;

	public static void Enter_stable_attributes() {
		boolean flag = false;
		String[] stable = null;
		System.out.println(" Attributes read: " + attributes.toString());
		System.out.println("Enter stable Attributes:");
		String s = scanner.next();
		if (s.split(",").length > 1) {
			stable = s.split(",");
			for (int j = 0; j < stable.length; j++) {
				if (!(attributes.contains(stable[j]))) {
					System.out.println("Invalid value");
					flag = true;
					break;
				}
			}
			if (flag == false) {
				stableattributes.addAll(Arrays.asList(stable));
				attributes.removeAll(stableattributes);
			}
		} else {
			if (!(attributes.contains(s))) {
				System.out.println("Invalid attribute");
			} else {
				stableattributes.add(s);
				attributes.removeAll(stableattributes);
			}
		}
/*		System.out.println("Stable Attribute(s): "
				+ stableattributes.toString());
		System.out.println("Available Attribute(s): "
				+ attributes.toString()); */
	}

	public static void Enter_decision_attribute() {
		System.out.println("Enter the Decision Attribute");
		String s = scanner.next();
		if (!(attributes.contains(s))) {
			System.out.println("Invalid attribute");
		} else {
			decisionattributes.add(s);
			index = atr_List.indexOf(s);
			attributes.removeAll(decisionattributes);
		}
	}

	public static void Decisionvalues_from_to(String args) {
		HashSet<String> set = new HashSet<String>();
		File data = new File(args);
		FileReader data_reader;
		BufferedReader data_buffer;
		try {
			data_reader = new FileReader(data);
			data_buffer = new BufferedReader(data_reader);
			String d = new String();
			while ((d = data_buffer.readLine()) != null)
				set.add(d.split(",")[index]);
			data_reader.close();
			data_buffer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println();
		Iterator<String> iterator = set.iterator();
		while (iterator.hasNext()) {
			dec_List.add(atr_List.get(index)
					+ iterator.next());
		}
/*		System.out.println("Available Decision Attributes: "
				+ dec_List.toString()); */
		System.out.println("Decision value from ");
		dec_from = scanner.next();
		System.out.println("Decision value to ");
		dec_to = scanner.next();
		System.out.println("Stable Attribute(s): "
				+ stableattributes.toString());
		System.out.println("Decision Attribute: "
				+ decisionattributes.toString());
/*		System.out.println("Decision From Attribute: " + dec_from);
		System.out.println("Decision To Attribute: " + dec_to);
		System.out.println("Flexible Attribute(s): " + attributes.toString()); */
	}

	public static void main(final String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		File attributefile = new File("cancerAttributes.txt");
		FileReader attribute_reader = new FileReader(attributefile);
		BufferedReader attribute_buffer = new BufferedReader(attribute_reader);
		String att = new String();
		while ((att = attribute_buffer.readLine()) != null) {
			attributes.addAll(Arrays.asList(att.split("\\s+")));
			atr_List.addAll(Arrays.asList(att.split("\\s+")));
		}
		int count = 0;
		attribute_reader.close();
		attribute_buffer.close();
		File dataset = new File("cancerdata.txt");
		FileReader data_reader = new FileReader(dataset);
		BufferedReader data_buffer = new BufferedReader(data_reader);
		@SuppressWarnings("unused")
		String d = new String();
		while ((d = data_buffer.readLine()) != null) {
			count++;
		}
		data_reader.close();
		data_buffer.close();
		Enter_stable_attributes();
		Enter_decision_attribute();
		Decisionvalues_from_to("cancerdata.txt");
		System.out.println("support: ");
		support = scanner.nextInt();
		System.out.println("Confidence %: ");
		confidence = scanner.nextInt();
		scanner.close();

		Configuration conf = new Configuration();

		conf.set("mapred.max.split.size", dataset.length() / 5 + "");
		conf.set("mapred.min.split.size", "0");

		conf.setInt("count", count);
		conf.setStrings("attributes", Arrays.copyOf(
				atr_List.toArray(),
				atr_List.toArray().length, String[].class));
		conf.setStrings("stable", Arrays.copyOf(stableattributes.toArray(),
				stableattributes.toArray().length, String[].class));
		conf.setStrings("decision", Arrays.copyOf(
				decisionattributes.toArray(),
				decisionattributes.toArray().length, String[].class));
		conf.setStrings("dec_from", dec_from);
		conf.setStrings("Decision To", dec_to);
		conf.setStrings("support", support + "");
		conf.setStrings("confidence", confidence + "");
		Job job1 = new Job(conf);	
		job1.setJarByClass(RF_Reducer.class);	
		job1.setMapperClass(RF_Reducer.Mapercode.class);
		job1.setReducerClass(RF_Reducer.ReducerCode.class);		
		job1.setNumReduceTasks(1);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		Path input_dir = new Path("Inputfiles/");		
		FileInputFormat.addInputPath(job1, input_dir);
		Path output_dir1 = new Path("Outputfiles/MapperOutput/");
		FileOutputFormat.setOutputPath(job1, output_dir1);		
		job1.waitForCompletion(true);
		Job job2 = new Job(conf);		
		job2.setJarByClass(RF_Mapper.class);		
		job2.setMapperClass(RF_Mapper.Mappercode.class);
		job2.setReducerClass(RF_Mapper.Reducercode.class);		
		job2.setNumReduceTasks(1);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, input_dir);
		Path output_dir2 = new Path("Outputfiles/ReducerOutput/");
		FileOutputFormat.setOutputPath(job2, output_dir2);		
		job2.waitForCompletion(true);
	}
}

