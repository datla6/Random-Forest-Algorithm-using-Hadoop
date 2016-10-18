/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author Administrator
 */
import javax.swing.*;
public class Display {


  public static void main(String[] args) {
      GUI myFrame=new GUI();
      
      JFrame f = new JFrame();
      f.setSize(1000, 800);
      f.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
      f.setVisible(true);         
      f.setTitle("Team7-Action Rules Extraction");
      f.getContentPane().add(myFrame);

  }
  
}
