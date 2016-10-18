package Team7;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RF_Mapper {
	public static class Mappercode extends
			Mapper<LongWritable, Text, Text, Text> {
		int C = 0;
		double support_threshold, confidence_threshold;
		String dec_attr, dec_from, dec_to;
		public static ArrayList<String> attributes, stable_Attributes,stable_Attribute_values, decision_from_to;
		ArrayList<HashSet<String>> Asc_Rules;
		static Map<ArrayList<String>, Integer> dataset;
		static Map<String, HashSet<String>> dist_attr_values,dec_values;
		static Map<HashSet<String>, HashSet<String>> attr_values;
		Map<ArrayList<ArrayList<String>>, ArrayList<String>> actionSets,act_derivd;
		Map<String, ArrayList<ArrayList<String>>> singleSet;
		public Mappercode() {
			super();
			attributes = new ArrayList<String>();
			stable_Attributes = new ArrayList<String>();
			stable_Attribute_values = new ArrayList<String>();
			decision_from_to = new ArrayList<String>();
			Asc_Rules = new ArrayList<HashSet<String>>();

			dataset = new HashMap<ArrayList<String>, Integer>();
			dist_attr_values = new HashMap<String, HashSet<String>>();
			dec_values = new HashMap<String, HashSet<String>>();
			attr_values = new HashMap<HashSet<String>, HashSet<String>>();
			actionSets = new HashMap<ArrayList<ArrayList<String>>, ArrayList<String>>();
			act_derivd = new HashMap<ArrayList<ArrayList<String>>, ArrayList<String>>();
			singleSet = new HashMap<String, ArrayList<ArrayList<String>>>();
		}
		@Override
		protected void setup(
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			attributes = new ArrayList<String>(Arrays.asList(context
					.getConfiguration().getStrings("attributes")));
stable_Attributes = new ArrayList<String>(Arrays.asList(context
		.getConfiguration().getStrings("stable")));
dec_attr = context.getConfiguration().getStrings(
		"decision")[0];
dec_from = context.getConfiguration().get("dec_from");
dec_to = context.getConfiguration().get("dec_to");
decision_from_to.add(dec_from);
decision_from_to.add(dec_to);
support_threshold = Double.parseDouble(context.getConfiguration().get(
		"support"));
confidence_threshold = Double.parseDouble(context.getConfiguration().get(
		"confidence"));

	super.setup(context);
}
@Override
protected void map(LongWritable key, Text inputValue,
		Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	dividedata(inputValue, C);
	// Getting stable attribute values
	for (int i = 0; i < stable_Attributes.size(); i++) {
		HashSet<String> distinctStableValues = dist_attr_values
				.get(stable_Attributes.get(i));

		for (String string : distinctStableValues) {
			if (!stable_Attribute_values.contains(string))
				stable_Attribute_values.add(string);
			else
				continue;
		}
	}
	dec_attr_values_enter();
	C++;		
}
private void dividedata(Text inputValue, int C) {
	int lineNo = C;
	String inputData = inputValue.toString();
	ArrayList<String> lineData = new ArrayList<String>(
			Arrays.asList(inputData.split("\t|,")));
	if (!empty_value_check(lineData)) {
		String key;
		lineNo++;
		ArrayList<String> A = new ArrayList<String>();
		HashSet<String> set;
		for (int j = 0; j < lineData.size(); j++) {
	String currentAttributeValue = lineData.get(j);
	String attributeName = attributes.get(j);
	key = attributeName + currentAttributeValue;
	A.add(key);
	if (dist_attr_values.containsKey(attributeName)) {
		set = dist_attr_values.get(attributeName);

	} else {
		set = new HashSet<String>();
	}
	set.add(key);
	dist_attr_values.put(attributeName, set);
		}

		if (!dataset.containsKey(A)) {
			dataset.put(A, 1);

			for (String listKey : A) {
				HashSet<String> mapKey = new HashSet<String>();
				mapKey.add(listKey);
				setMap(attr_values, mapKey, lineNo);
			}
		} else
			dataset.put(A, dataset.get(A) + 1);

	}
}
private static boolean empty_value_check(
		ArrayList<String> lineData) {
	return lineData.contains("");
}
private static void setMap(
		Map<HashSet<String>, HashSet<String>> values,
		HashSet<String> key, int lineNo) {
	HashSet<String> B = new HashSet<String>();

	if (values.containsKey(key)) {
		B.addAll(values.get(key));
	}

	B.add("x" + lineNo);
	values.put(key, B);
}

private void dec_attr_values_enter() {
	HashSet<String> distinctDecisionValues = dist_attr_values
			.get(dec_attr);
	for (String value : distinctDecisionValues) {
		HashSet<String> newHash = new HashSet<String>();
		HashSet<String> finalHash = new HashSet<String>();
		newHash.add(value);

		if (dec_values.containsKey(value)) {
			finalHash.addAll(dec_values.get(value));
		}
		if (attr_values.containsKey(newHash))
			finalHash.addAll(attr_values.get(newHash));

		dec_values.put(value, finalHash);
		// attr_values.remove(newHash);
	}
}

@Override
protected void cleanup(
		Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	Enter_attr_values();

	while (!actionSets.isEmpty()) {
		periodic_events(context);
	}

	super.cleanup(context);
}

private void Enter_attr_values() {
	ArrayList<String> processedStableAttributes = new ArrayList<String>();

	for (Map.Entry<HashSet<String>, HashSet<String>> set : attr_values
			.entrySet()) {
		int support = set.getValue().size();
		if (support >= support_threshold) {
			ArrayList<ArrayList<String>> outerSet = new ArrayList<ArrayList<String>>();
			ArrayList<String> innerSet = new ArrayList<String>();
			ArrayList<String> attributes = new ArrayList<String>();

			String P_Attr = null;

			for (String element : set.getKey()) {
				innerSet.add(element);
				innerSet.add(element);
				outerSet.add(innerSet);

				P_Attr = get_attr_name(element);
				attributes.add(P_Attr);
				attributes.add(String.valueOf(support));
			}

			if (P_Attr != null) {
				actionSets.put(outerSet, attributes);

				ArrayList<ArrayList<String>> forValue = new ArrayList<ArrayList<String>>();
				if (singleSet.get(P_Attr) != null) {
					forValue = singleSet.get(P_Attr);
				}
				forValue.add(innerSet);
				singleSet.put(P_Attr, forValue);
if (!isStable(P_Attr)&& !processedStableAttributes.contains(P_Attr)) {
ArrayList<String> dist_attr_valuesAAR = new ArrayList<String>();
dist_attr_valuesAAR.addAll(dist_attr_values.get(P_Attr));
for (int i = 0; i < dist_attr_valuesAAR
		.size(); i++) {
	for (int j = 0; j < dist_attr_valuesAAR
			.size(); j++) {
		if (i != j) {
			HashSet<String> left = new HashSet<String>(
					Arrays.asList(dist_attr_valuesAAR
							.get(i)));
			HashSet<String> right = new HashSet<String>(
			Arrays.asList(dist_attr_valuesAAR.get(j)));
			support = Math.min(
			attr_values.get(left).size(),
			attr_values.get(right).size());
			if (support >= support_threshold) {
				processedStableAttributes.add(P_Attr);
				outerSet = new ArrayList<ArrayList<String>>();
				innerSet = new ArrayList<String>();
				innerSet.addAll(left);
				innerSet.addAll(right);
				outerSet.add(innerSet);
				attributes.set(1,String.valueOf(support));
				actionSets.put(outerSet,attributes);
				forValue = singleSet.get(P_Attr);
				forValue.add(innerSet);
				singleSet.put(P_Attr,forValue);

}}}}}}}}}
public static String get_attr_name(String value1) {
	for (Map.Entry<String, HashSet<String>> entryValue : dist_attr_values
			.entrySet()) {
		if (entryValue.getValue().contains(value1)) {
			return entryValue.getKey();
		}
	}
	return null;
}
public boolean isStable(String value) {
	if (stable_Attribute_values.containsAll(dist_attr_values
			.get(value)))
		return true;
	else
		return false;
}
private void periodic_events(
		Mapper<LongWritable, Text, Text, Text>.Context context) {
	act_derivd = new HashMap<ArrayList<ArrayList<String>>, ArrayList<String>>();
	for (Map.Entry<ArrayList<ArrayList<String>>, ArrayList<String>> mainSet : actionSets.entrySet()) {
ArrayList<ArrayList<String>> mainKey = mainSet.getKey();
ArrayList<String> mainValue = mainSet.getValue();
for (Map.Entry<String, ArrayList<ArrayList<String>>> attrSet : singleSet
		.entrySet()) {
	String attrKey = attrSet.getKey();
	if (((mainValue.contains(dec_attr) && mainKey
			.contains(decision_from_to)) || (attrKey
			.equals(dec_attr) && attrSet.getValue()
			.contains(decision_from_to)))
			&& !mainValue.contains(attrKey)) {
		for (ArrayList<String> extraSet : attrSet.getValue()) {
			ArrayList<String> toCheckIn = new ArrayList<String>();
			ArrayList<String> toCheckOut = new ArrayList<String>();
			ArrayList<String> newValue = new ArrayList<String>();
			ArrayList<ArrayList<String>> newKey = new ArrayList<ArrayList<String>>();
			newKey.addAll(mainKey);
			newKey.add(extraSet);
			newValue.addAll(mainValue.subList(0,
					mainValue.size() - 1));
			newValue.add(attrKey);
	HashSet<String> newAssociation = new HashSet<String>();
	for (ArrayList<String> checkMultipleValues : newKey) {
		String toAddIntoAssociations = "";
		String left = checkMultipleValues.get(0);
		String right = checkMultipleValues.get(1);
		toCheckIn.add(left);
		toAddIntoAssociations += left + " -> ";
						toCheckOut.add(right);
						toAddIntoAssociations += right;
						newAssociation.add(toAddIntoAssociations);
					}
					if (!Asc_Rules.contains(newAssociation)) {
						int support = Math.min(
								LERS_findsupport(toCheckIn),
								LERS_findsupport(toCheckOut));
						newValue.add(String.valueOf(support));
						if (support >= support_threshold) {
							act_derivd.put(newKey, newValue);
							periodic_actions_print(newKey, newValue,
									context);
							Asc_Rules.add(newAssociation);
}}}}}}
	actionSets.clear();
	actionSets.putAll(act_derivd);
	act_derivd.clear();
}
private static int LERS_findsupport(ArrayList<String> A) {
	int count = 0;
	for (Map.Entry<ArrayList<String>, Integer> entry : dataset.entrySet()) {
		if (entry.getKey().containsAll(A)) {
			count += entry.getValue();
		}
	}
	return count;
}
private void periodic_actions_print(
		final ArrayList<ArrayList<String>> key,
		final ArrayList<String> value,
		final Mapper<LongWritable, Text, Text, Text>.Context context) {
	if (value.contains(dec_attr)) {
		String rule = "", decision = "", dec_from = "", dec_to = "";
int count = 0;
ArrayList<String> actionFrom = new ArrayList<String>();
ArrayList<String> actionTo = new ArrayList<String>();
for (ArrayList<String> list : key) {
	if (value.get(count).equals(dec_attr)) {
		dec_from = list.get(0);
		dec_to = list.get(1);
		decision = "(" + value.get(count) + "," + dec_from
		+ " ->  " + dec_to + ")";
} else {
	if (!rule.equals("")) {
	rule += "^";
}
rule += "(" + value.get(count) + "," + list.get(0)
		+ " ->  " + list.get(1) + ")";
		actionFrom.add(list.get(0));
		actionTo.add(list.get(1));
	}
	count++;
}
if (!rule.equals("")
	&& !stable_Attribute_values.containsAll(actionFrom)) {
rule += " ==> " + decision;
final String finalRule = rule;
final String finalDecisionFrom = dec_from;
final String finalDecisionTo = dec_to;
String suppConf = AscRule_Spprt(actionFrom,
		actionTo, finalDecisionFrom, finalDecisionTo,
		support_threshold, confidence_threshold);
if (!suppConf.equals("")) {
try {
	Text key1 = new Text(finalRule);
	Text value1 = new Text(suppConf);
	if (key1.toString().indexOf(dec_attr) < key1
			.toString().indexOf(decision_from_to.get(0)))
		if (key1.toString().indexOf(
				decision_from_to.get(0)) < key1
				.toString().indexOf(
						decision_from_to.get(1)))
			context.write(key1, value1);
} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
} catch (InterruptedException e) {
	// TODO Auto-generated catch block
					e.printStackTrace();
}}}}}
public String AscRule_Spprt(ArrayList<String> actionFrom,
		ArrayList<String> actionTo, String dec_from,
		String dec_to, double minSupp, double minConf) {
	String supportConfidence = new String();

	ArrayList<String> leftRule = new ArrayList<String>();
	ArrayList<String> rightRule = new ArrayList<String>();

	leftRule.addAll(actionFrom);
	leftRule.add(dec_from);
	rightRule.addAll(actionTo);
	rightRule.add(dec_to);
	double leftRuleSupport = LERS_findsupport(leftRule);
	double rightRuleSupport = LERS_findsupport(rightRule);
	double leftSupport = LERS_findsupport(actionFrom);
	double rightSupport = LERS_findsupport(actionTo);

	double support = Math.min(leftRuleSupport, rightRuleSupport);
	double confidence = (leftRuleSupport / leftSupport)
			* (rightRuleSupport / rightSupport) * 100;
	if (confidence >= minConf && support >= minSupp)
		supportConfidence = support + "," + confidence;

		return supportConfidence;
	}
}
	
public static class Reducercode extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
ArrayList<Double> support = new ArrayList<Double>();
ArrayList<Double> confidence = new ArrayList<Double>();
ArrayList<Text> valueList = new ArrayList<Text>();
int mapper = Integer.parseInt(context.getConfiguration().get(
		"mapred.map.tasks"));
int min_presence = mapper / 2;
int sum = 0;
DecimalFormat df = new DecimalFormat("###.##");
for (Text val : values) {
	sum++;
	valueList.add(val);
	support.add(Double.valueOf(df.format(Double.parseDouble(val
			.toString().split(",")[0]))));
confidence.add(Double.valueOf(df.format(Double.parseDouble(val
		.toString().split(",")[1]))));
}
if (sum >= min_presence) {
	for (int i = 0; i < valueList.size(); i++)
		context.write(key, new Text("[Support: " + support.get(i)
	+ ", Confidence: " + confidence.get(i) + "%]"));
}}
}
}
