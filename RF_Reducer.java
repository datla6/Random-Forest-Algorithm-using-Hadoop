package Team7;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RF_Reducer {
	public static class Mapercode extends
			Mapper<LongWritable, Text, Text, Text> {
static Map<ArrayList<String>, Integer> dataset;
static Map<String, HashSet<String>> dist_attr_values, dec_values;
static Map<HashSet<String>, HashSet<String>> attr_values, red_attr_values;
static Map<ArrayList<String>, HashSet<String>> mrkd_values,possibleRules;
static Map<ArrayList<String>, String> certainRules;int C = 0;
boolean false_param;
double support_threshold, confidence_threshold;
String dec_attr, dec_from, dec_to;
public static ArrayList<String> attributes, stable_attr, stable_attr_values;
public static ArrayList<ArrayList<String>> A_Rules;
public Mapercode() {
	super();
	stable_attr = new ArrayList<String>();
	stable_attr_values = new ArrayList<String>();
	attr_values = new HashMap<HashSet<String>, HashSet<String>>();
	red_attr_values = new HashMap<HashSet<String>, HashSet<String>>();
	dist_attr_values = new HashMap<String, HashSet<String>>();
	dec_values = new HashMap<String, HashSet<String>>();
	certainRules = new HashMap<ArrayList<String>, String>();
	mrkd_values = new HashMap<ArrayList<String>, HashSet<String>>();
	false_param = false;
	dataset = new HashMap<ArrayList<String>, Integer>();
	A_Rules = new ArrayList<ArrayList<String>>();
	attributes = new ArrayList<String>();
	possibleRules = new HashMap<ArrayList<String>, HashSet<String>>();
}
@Override
protected void setup(
		Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	attributes = new ArrayList<String>(Arrays.asList(context
			.getConfiguration().getStrings("attributes")));
stable_attr = new ArrayList<String>(Arrays.asList(context
		.getConfiguration().getStrings("stable")));
dec_attr = context.getConfiguration().getStrings(
		"decision")[0];
dec_from = context.getConfiguration().get("dec_from");
dec_to = context.getConfiguration().get("dec_to");
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
	divideData(inputValue, C);
for (int i = 0; i < stable_attr.size(); i++) {
	HashSet<String> distinctStableValues = dist_attr_values
			.get(stable_attr.get(i));

	for (String string : distinctStableValues) {
		if (!stable_attr_values.contains(string))
			stable_attr_values.add(string);
		else
			continue;
	}
}
Enter_dec_attr_values();
C++;
}

// SPLITTING THE DATA SET
private void divideData(Text inputValue, int C) {
int lineNo = C;
String inputData = inputValue.toString();
ArrayList<String> lineData = new ArrayList<String>(Arrays.asList(inputData.split("\t|,")));
if (!Empty_Value_Check(lineData)) {
	String key;
	lineNo++;
	ArrayList<String> T = new ArrayList<String>();
	HashSet<String> set;
	for (int j = 0; j < lineData.size(); j++) {
		String currentAttributeValue = lineData.get(j);
		String attributeName = attributes.get(j);
		key = attributeName + currentAttributeValue;
		T.add(key);
		if (dist_attr_values.containsKey(attributeName)) {
			set = dist_attr_values.get(attributeName);

		} else {
			set = new HashSet<String>();
		}
		set.add(key);
		// Setting attribute values to the corresponding attribute
			dist_attr_values.put(attributeName, set);
		}

		if (!dataset.containsKey(T)) {
			dataset.put(T, 1);

			for (String listKey : T) {
				HashSet<String> mapKey = new HashSet<String>();
				mapKey.add(listKey);
				map_set(attr_values, mapKey, lineNo);
			}
		} else
			dataset.put(T, dataset.get(T) + 1);
	}

}
private static boolean Empty_Value_Check(
		ArrayList<String> lineData) {
	return lineData.contains("");
}
private static void map_set(
		Map<HashSet<String>, HashSet<String>> values,
		HashSet<String> key, int lineNo) {
	HashSet<String> tempSet = new HashSet<String>();

	if (values.containsKey(key)) {
		tempSet.addAll(values.get(key));
	}

	tempSet.add("x" + lineNo);
	values.put(key, tempSet);
}

private void Enter_dec_attr_values() {
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
		attr_values.remove(newHash);
	}
}
protected void cleanup(
		final Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	LERS_Implement();

	generateActionRules(context);

	super.cleanup(context);
}

//LERS IMPLEMENATION
private void LERS_Implement() {
	int loopCount = 0;
	while (!attr_values.isEmpty()) {
		for (Map.Entry<HashSet<String>, HashSet<String>> set : attr_values
				.entrySet()) {
			ArrayList<String> setKey = new ArrayList<String>();
			setKey.addAll(set.getKey());
			HashSet<String> setValue = set.getValue();
			if (set.getValue().isEmpty()) {
				continue;
			} else {
				for (Map.Entry<String, HashSet<String>> decisionSet : dec_values
						.entrySet()) {
					if (decisionSet.getValue().containsAll(
							set.getValue())) {
						certainRules.put(setKey, decisionSet.getKey());
						mrkd_values.put(setKey, set.getValue());
						break;
					}
				}
			}
			if (!mrkd_values.containsKey(setKey)) {
				HashSet<String> possibleRulesSet = new HashSet<String>();
				for (Map.Entry<String, HashSet<String>> decisionSet : dec_values
						.entrySet()) {
					// if(LERS_support(setKey,decisionSet.getKey())!=0)
					possibleRulesSet.add(decisionSet.getKey());
				}
				if (possibleRulesSet.size() > 0)
					possibleRules.put(setKey, possibleRulesSet);
			}
		}
		rem_mrkdvalues();
		grp_possible_Rules();
	}
}

// USING LERS AND CALCULATING SUPPORT AND CONFIDENCE
static int LERS_support(ArrayList<String> key, String value) {
	ArrayList<String> T = new ArrayList<String>();
	for (String val : key) {
		if (!val.equals(""))
		T.add(val);
}
if (!value.equals(""))
		T.add(value);
	return cal_LERSsupport(T);
}
private static int cal_LERSsupport(ArrayList<String> T) {
	int count = 0;
	for (Map.Entry<ArrayList<String>, Integer> entry : dataset.entrySet()) {
		if (entry.getKey().containsAll(T)) {
			count += entry.getValue();
		}
	}
	return count;
}
// CALCULATE  CONFIDENCE
static String cal_LERSconfidence(ArrayList<String> key,
		String value) {
	int num = LERS_support(key, value);
	int den = LERS_support(key, "");
int confidence = 0;
if (den != 0) {
	confidence = (num * 100) / den;
}
	return String.valueOf(confidence);
}
private static void rem_mrkdvalues() {
	for (Map.Entry<ArrayList<String>, HashSet<String>> markedSet : mrkd_values
			.entrySet()) {
		attr_values.remove(new HashSet<String>(markedSet.getKey()));
	}
}
private static void grp_possible_Rules() {
	Set<ArrayList<String>> keySet = possibleRules.keySet();
	ArrayList<ArrayList<String>> keyList = new ArrayList<ArrayList<String>>();
	keyList.addAll(keySet);
	for (int i = 0; i < possibleRules.size(); i++) {
		for (int j = (i + 1); j < possibleRules.size(); j++) {
			HashSet<String> combinedKeys = new HashSet<String>(
					keyList.get(i));
			combinedKeys.addAll(new HashSet<String>(keyList.get(j)));
			if (!verify_group(combinedKeys)) {
				grp_attr_values(combinedKeys);
			}
		}
	}
	delete_repeated_values();
	delete_attr_values();
	possibleRules.clear();
}
public static boolean verify_group(HashSet<String> combinedKeys) {
	ArrayList<String> combinedKeyAttributes = new ArrayList<String>();
	for (Map.Entry<String, HashSet<String>> singleAttribute : dist_attr_values
			.entrySet()) {
		for (String key : combinedKeys) {
			if (singleAttribute.getValue().contains(key)) {
				if (!combinedKeyAttributes.contains(singleAttribute
						.getKey()))
					combinedKeyAttributes.add(singleAttribute.getKey());
				else
					return true;
			}
		}
	}
	return false;
}
private static void grp_attr_values(HashSet<String> combinedKeys) {
	HashSet<String> combinedValues = new HashSet<String>();
	for (Map.Entry<HashSet<String>, HashSet<String>> attributeValue : attr_values
			.entrySet()) {
		if (combinedKeys.containsAll(attributeValue.getKey())) {
			if (combinedValues.isEmpty()) {
				combinedValues.addAll(attributeValue.getValue());
			} else {
				combinedValues.retainAll(attributeValue.getValue());
			}
		}
	}
	if (combinedValues.size() != 0)
		red_attr_values.put(combinedKeys, combinedValues);
}
private static void delete_repeated_values() {
	HashSet<String> mark = new HashSet<String>();
	for (Map.Entry<HashSet<String>, HashSet<String>> reducedAttributeValue : red_attr_values
			.entrySet()) {
		for (Map.Entry<HashSet<String>, HashSet<String>> attributeValue : attr_values
				.entrySet()) {

			if (attributeValue.getValue().containsAll(
					reducedAttributeValue.getValue())
					|| reducedAttributeValue.getValue().isEmpty()) {
				mark.addAll(reducedAttributeValue.getKey());
			}
		}
	}
	red_attr_values.remove(mark);
}
private static void delete_attr_values() {
	attr_values.clear();
	for (Map.Entry<HashSet<String>, HashSet<String>> reducedAttributeValue : red_attr_values
			.entrySet()) {
		attr_values.put(reducedAttributeValue.getKey(),
				reducedAttributeValue.getValue());
	}
	red_attr_values.clear();
}
// ACTION RULE EXTRACTION
public ArrayList<String> generateActionRules(
		Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	ArrayList<String> actionFrom, actionTo, rules;
	ArrayList<String> actions = null;
	String rule = "";
rules = new ArrayList<String>();
for (Map.Entry<ArrayList<String>, String> certainRules1 : certainRules
		.entrySet()) {
	String certainRules1Value = certainRules1.getValue();
	if (certainRules1Value.equals(dec_from)) {
	for (Map.Entry<ArrayList<String>, String> certainRules2 : certainRules
			.entrySet()) {
		ArrayList<String> certainRules1Key = certainRules1
				.getKey();
		ArrayList<String> certainRules2Key = certainRules2
				.getKey();
		if ((certainRules1Key.equals(certainRules2Key))
				|| (!certainRules2.getValue()
						.equals(dec_to))
				|| !checkStableAttributes(certainRules1Key,
						certainRules2Key)) {
			continue;
		} else {
			String prm_attr = "";
	if (checkRulesSubSet(certainRules1Key,
			certainRules2Key)) {
		ArrayList<String> checkCertainValues1 = certainRules1
				.getKey();
		ArrayList<String> T = new ArrayList<String>();
		rule = "";
		actionFrom = new ArrayList<String>();
		actionTo = new ArrayList<String>();
		actions = new ArrayList<String>();
		for (String value1 : checkCertainValues1) {
	if (stable_attr_values.contains(value1)) {
		if (!actionTo.contains(value1)) {
			rule = formRule(rule, value1,
					value1);
			actionFrom.add(value1);
			actionTo.add(value1);
			actions.add(getAction(value1,
					value1));
		}
		continue;
			} else {
		prm_attr = getAttributeName(value1);
		ArrayList<String> checkCertainValues2 = certainRules2
				.getKey();
		for (String value2 : checkCertainValues2) {
			if (stable_attr_values
					.contains(value2)) {
				if (!actionTo.contains(value2)) {
					rule = formRule(rule,
							value2, value2);

					actionFrom.add(value2);
					actionTo.add(value2);
					actions.add(getAction(
							value2, value2));
				}
			} else if (!(getAttributeName(value2)
					.equals(prm_attr))) {
				T.add(value2);
			} else if (getAttributeName(value2)
					.equals(prm_attr)
					&& !actionTo
							.contains(value2)) {

				rule = formRule(rule, value1,
						value2);
				actionFrom.add(value1);
				actionTo.add(value2);
				actions.add(getAction(value1,
						value2));
		}
		}
	}
}
for (String missedValues : T) {
	if (!actionTo.contains(missedValues)) {
		rule = formRule(rule, "", missedValues);
		actionFrom.add("");
		actionTo.add(missedValues);
		actions.add(getAction("", missedValues));
	}
}
		print_Act_rules(actionFrom, actionTo, actions,
				rule, context);
		print_rules(actionFrom, actionTo, context);
}}}}}
	return rules;
}
private static boolean checkStableAttributes(ArrayList<String> key,
		ArrayList<String> key2) throws IOException,
		InterruptedException {
	List<String> stable_attrList1 = new ArrayList<String>();
	List<String> stable_attrList2 = new ArrayList<String>();

	for (String value : key) {
		if (stable_attr_values.contains(value))
			stable_attrList1.add(value);
	}
	for (String value : key2) {
		if (stable_attr_values.contains(value))
			stable_attrList2.add(value);
	}
	if (stable_attrList2.containsAll(stable_attrList1))
		return true;
	else
		return false;
}
private boolean checkRulesSubSet(ArrayList<String> certainRules1,
		ArrayList<String> certainRules2) {
	ArrayList<String> prm_attrs1 = new ArrayList<String>();
	ArrayList<String> prm_attrs2 = new ArrayList<String>();
	for (String string : certainRules1) {
		String attributeName = getAttributeName(string);
		if (!isStable(attributeName))
			prm_attrs1.add(attributeName);
	}
	for (String string : certainRules2) {
		String attributeName = getAttributeName(string);

		if (!isStable(attributeName))
			prm_attrs2.add(attributeName);
	}
	if (prm_attrs2.containsAll(prm_attrs1))
		return true;
	else
		return false;
}
public static String getAttributeName(String value1) {
	for (Map.Entry<String, HashSet<String>> entryValue : dist_attr_values
			.entrySet()) {
		if (entryValue.getValue().contains(value1)) {
			return entryValue.getKey();
		}
	}
	return null;
}
private static String formRule(String rule, String value1, String value2) {
	if (!rule.isEmpty())
		rule += "^";
rule += "(" + getAttributeName(value2) + ","
+ getAction(value1, value2) + ")";
	return rule;
}
private static String getAction(String left, String right) {
	return left + "->" + right;
}
private void print_Act_rules(final ArrayList<String> actionFrom,
		final ArrayList<String> actionTo,
		final ArrayList<String> actions, final String rule,
		final Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	final int support = LERS_support(actionTo, dec_to);
	if (support != 0) {
		final String oldConfidence = String.valueOf((Integer
				.parseInt(cal_LERSconfidence(actionFrom,
						dec_from))
				* Integer.parseInt(cal_LERSconfidence(actionTo,
						dec_to)) / 100));
		final String newConfidence = cal_LERSconfidence(actionTo,
				dec_to);
		if ((support >= support_threshold && Double.parseDouble(oldConfidence) >= confidence_threshold)
				&& !oldConfidence.equals("0")
&& !newConfidence.equals("0")) {
if (actions != null) {
	Collections.sort(actions);
	if (!A_Rules.contains(actions)) {
		A_Rules.add(actions);
		try {
			Text key1 = new Text(rule + " ==> " + "("
				+ dec_attr + ","
				+ dec_from + "->" + dec_to
				+ ")");
		Text value1 = new Text(support + ":"
				+ newConfidence);
		context.write(key1, value1);

	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
						e.printStackTrace();
}
}}}}}
private void print_rules(final ArrayList<String> actionFrom,
		final ArrayList<String> actionTo,
		final Mapper<LongWritable, Text, Text, Text>.Context context)
		throws IOException, InterruptedException {
	ArrayList<String> stableValues = get_values_stable(actionTo);
	ArrayList<String> attr_values = getAttributeValues(
			stableValues, dec_from, actionFrom);
	ArrayList<String> toBeAddedAttributes = getNewAttributes(
			actionFrom, actionTo, stableValues);
	ArrayList<String> tempAttributeValues;
	for (String attributeValue : toBeAddedAttributes) {
		tempAttributeValues = new ArrayList<String>();
		tempAttributeValues.add(attributeValue);
		ArrayList<String> checkList = getAttributeValues(stableValues,
				"", tempAttributeValues);
if (attr_values.containsAll(checkList)
		&& !checkList.isEmpty()) {
	String subRule = new String();
	ArrayList<String> subActionFrom = new ArrayList<String>();
	ArrayList<String> subActionTo = new ArrayList<String>();
	ArrayList<String> subActions = new ArrayList<String>();
	if (isStable(getAttributeName(attributeValue))) {
		subActionFrom.addAll(actionFrom);
		subActionFrom.add(attributeValue);
		subActionTo.addAll(actionTo);
		subActionTo.add(attributeValue);
	} else {
		subActionFrom = get_sub_action_from(actionFrom, actionTo,
				attributeValue);
		subActionTo.addAll(actionTo);
	}
	subRule = get_sub_rule(subActionFrom, subActionTo, subActions);
	try {
		print_Act_rules(subActionFrom, subActionTo, subActions,
				subRule, context);
	} catch (IOException e) {
		// TODO Auto-generated catch block
	e.printStackTrace();
} catch (InterruptedException e) {
	// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
private ArrayList<String> getNewAttributes(
		ArrayList<String> actionFrom, ArrayList<String> actionTo,
		ArrayList<String> stableValues) {
	ArrayList<String> stable_attr = new ArrayList<String>();
	ArrayList<String> flexibleAttributes = new ArrayList<String>();
	ArrayList<String> newAttributes = new ArrayList<String>();
	for (String string : stableValues) {
		stable_attr.add(getAttributeName(string));
	}
	for (String string : actionTo) {
		flexibleAttributes.add(getAttributeName(string));
	}
	for (Map.Entry<String, HashSet<String>> mapValue : dist_attr_values
			.entrySet()) {
		String mapKey = mapValue.getKey();
		HashSet<String> mapValues = mapValue.getValue();

		if (mapKey.equals(dec_attr)
				|| (stable_attr.size() != 0 && stable_attr
						.contains(mapKey))) {
			continue;
		} else if (isStable(mapKey)) {
			newAttributes.addAll(mapValues);
		} else if (!flexibleAttributes.isEmpty()
				&& flexibleAttributes.contains(mapKey)) {

			for (String setValue : mapValues) {
				if (!actionFrom.contains(setValue)
						&& !actionTo.contains(setValue)) {
				newAttributes.add(setValue);
		}
	}
}
}
	return newAttributes;
}
private ArrayList<String> get_values_stable(ArrayList<String> actionFrom) {
	ArrayList<String> stableValues = (ArrayList<String>) stable_attr_values;
	ArrayList<String> toBeAdded = new ArrayList<String>();

	for (String value : actionFrom) {
		if (stableValues.contains(value)) {
			toBeAdded.add(value);
		} else {
			continue;
		}
	}
	return toBeAdded;
}
private ArrayList<String> getAttributeValues(
		ArrayList<String> stableValues, String dec_from,
		ArrayList<String> actionFrom) {
	ArrayList<String> temp = new ArrayList<String>();
	ArrayList<String> attr_values = new ArrayList<String>();
	int C = 0;
	temp.addAll(stableValues);
	for (String from : actionFrom) {
		if (!from.equals("")) {
		temp.add(from);
	}
}
if (!dec_from.equals(""))
	temp.add(dec_from);
for (Map.Entry<ArrayList<String>, Integer> dataset : dataset.entrySet()) {
	C++;
	if (dataset.getKey().containsAll(temp)) {
		attr_values.add("x" + C);
		}
	}
	return attr_values;
}
private ArrayList<String> get_sub_action_from(
		ArrayList<String> actionFrom, ArrayList<String> actionTo,
		String alternateActionFrom) {
	ArrayList<String> finalActionFrom = new ArrayList<String>();
	HashSet<String> checkSameSet;
	for (int i = 0; i < actionTo.size(); i++) {
		checkSameSet = new HashSet<String>();
		checkSameSet.add(alternateActionFrom);
		checkSameSet.add(actionTo.get(i));
		if (verify_group(checkSameSet)) {
			finalActionFrom.add(alternateActionFrom);
		} else {
			if (i < actionFrom.size()) {
				finalActionFrom.add(actionFrom.get(i));
			}
		}
	}
	return finalActionFrom;
}
private String get_sub_rule(ArrayList<String> subActionFrom,
		ArrayList<String> subActionTo, ArrayList<String> subActions) {
	String rule = "";

		for (int i = 0; i < subActionFrom.size(); i++) {
			rule = formRule(rule, subActionFrom.get(i), subActionTo.get(i));
			subActions.add(getAction(subActionFrom.get(i),
					subActionTo.get(i)));
		}

		return rule;
	}
	public boolean isStable(String value) {
		if (stable_attr_values.containsAll(dist_attr_values
				.get(value)))
			return true;
		else
			return false;
	}
}
public static class ReducerCode extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {

		ArrayList<Double> support = new ArrayList<Double>();
		ArrayList<Double> confidence = new ArrayList<Double>();
		int mapper = Integer.parseInt(context.getConfiguration().get(
				"mapred.map.tasks"));
int min_presence = mapper / 2;
int sum = 0;
for (Text val : values) {
	sum++;
	support.add(Double.parseDouble(val.toString().split(":")[0]));
confidence
		.add(Double.parseDouble(val.toString().split(":")[1]));
}
if (sum >= min_presence)
	context.write(key, new Text(sum + "" + " [Support: "
+ Collections.max(support) + ", Confidence: "
+ Collections.max(confidence) + "%]"));
		}
	}
}