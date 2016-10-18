/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author Administrator
 */
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class DataInput {
  private int support;
  private int confidence;
  private String dataFilePath;
  private String decisionAttribute;
  private String decisionFrom;
  private String decisionTo;
  private List<String> attributeNames = new ArrayList<String>();
  private List<String> stableAttributes = new ArrayList<String>();
  private List<String> flexibleAttributes = new ArrayList<String>();
  private ArrayList<ArrayList<String>> data = new ArrayList<ArrayList<String>>();

  public void SetDataFilePath(String dataFilePath) {
      this.dataFilePath = dataFilePath;
      ReadData();
  }

  public void ReadData() {
      String[] row;
//      System.out.println(attributeNames.size());
      for (String attribute:attributeNames) {
          data.add(new ArrayList<String>());
      }
      
      try {
          Scanner input = new Scanner(new File(dataFilePath));
          
          while (input.hasNextLine()) {
              row = input.nextLine().split(",");
              for (int i=0; i<row.length; i++) {
              	  data.get(i).add(row[i].trim());

              }
          }
      } catch (FileNotFoundException e) {
          printMessage("File Not Found");
          e.printStackTrace();
      }
  }

  public void SetSupport(int support) {
      this.support = support;
  }

  public void SetConfidence(int confidence) {
      this.confidence = confidence;
  }

  public void SetAttributeNames(String[] attributeNames) {
      this.attributeNames = new ArrayList<String>(Arrays.asList(attributeNames));
      this.flexibleAttributes = new ArrayList<String>(Arrays.asList(attributeNames));
  }
  
  public void SetDecisionAttribute(String decisionAttribute) {
      this.decisionAttribute = decisionAttribute;
      this.flexibleAttributes.remove(decisionAttribute);
  }

  public void SetStableAttribute(int[] stableAttributesIndex) {
      for (int attributeIndex:stableAttributesIndex) {
          this.stableAttributes.add(this.attributeNames.get(attributeIndex));
      }

      for (String attribute:this.stableAttributes) {
          if(this.flexibleAttributes.contains(attribute)) {
              this.flexibleAttributes.remove(attribute);
          }
      }
  }

	public void SetDecisionFromValue(String decisionFrom)
  {
      int decisionAttributeIndex = attributeNames.indexOf(decisionAttribute);
      if (data.get(decisionAttributeIndex).contains(decisionFrom)) {
          this.decisionFrom = decisionFrom;
      }
		else {
			printMessage("Invalid value.");
		}
	}

  public void SetDecisionToValue(String decisionTo)
  {
      int decisionAttributeIndex = attributeNames.indexOf(decisionAttribute);
              
      if (data.get(decisionAttributeIndex).contains(decisionTo)) {
          this.decisionTo = decisionTo;
      }
      else {
          printMessage("Invalid value.");
      }
  }

  public ArrayList<ArrayList<String>> GetData() { return data; }
  public int GetSupport() { return support; }
  public int GetConfidence() { return confidence; }
  public List<String> GetAttributeNames() { return attributeNames; }
  public String GetDecisionAttribute() { return decisionAttribute; }
  public List<String> GetStableAttributes() { return stableAttributes; }
  public List<String> GetFlexibleAttributes() { return flexibleAttributes; }
  public String GetDecisionFromValue() { return decisionFrom; }
  public String GetDecisionToValue() { return decisionTo; }

  public void printMessage(String content) {
      System.out.println(content);
  }
             
  public void printList(List<String> list) {
      Iterator iterate = list.iterator();
              
      while(iterate.hasNext()) {
          printMessage(iterate.next().toString());
      }
  }
}
