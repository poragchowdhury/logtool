/*
 * Copyright (c) 2012 by the original author
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.powertac.logtool.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import javax.print.DocFlavor.INPUT_STREAM;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.powertac.common.Broker;
import org.powertac.common.ClearedTrade;
import org.powertac.common.MarketTransaction;
import org.powertac.common.TimeService;
import org.powertac.common.msg.SimStart;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.repo.TimeslotRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.example.UnitCostAnalyzer.BrokerData;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Logtool Analyzer that reads ClearedTrade instances as they arrive and
 * builds an array for each timeslot giving all the market clearings for
 * that timeslot, indexed by leadtime. The output data file has one 
 * line/timeslot formatted as<br>
 * timeslot,day-of-week,hour-of-day,[mwh price],[mwh price] ...<br>
 * Each line has 24 entries, assuming that each timeslot is open for trading
 * 24 times.
 * 
 * If the option '--no-headers' is given, the first three fields are omitted.
 * 
 * Usage: MktPriceStats [--no-headers] state-log-filename output-data-filename
 * 
 * @author Porag Chowdhurys
 */
public class ResultsHATrading
{
	static private Logger log = LogManager.getLogger(MktPriceStats.class.getName());

	// Data
	private HashMap <String, Data> brokerDr;
	private HashMap <Integer, String> brokerID;
	private PrintWriter output = null;
	private String inputFilename = "";
	private String outputFilename = "clearedTrades.data";
	
	private double N = 0.0;
	private int HA = 24;
	private int ITEMS = (4*HA)+1+4; // 1 for Broker name, 2 for Balancing Mkt transaction
	private int n = 0;
	private boolean firstTime = true;
	private String SPOT = "SPOT";
	//private int length = 0;
	/**
	 * Main method just creates an instance and passes command-line args to
	 * its inherited cli() method.
	 */
	public static void main (String[] args)
	{
		new ResultsHATrading().run2(args);
	}
	
	/*
	 * Dr. Christopher Kiekintveld 
	 * */
	public void run2(String[] args){
		inputFilename = args[0];
		outputFilename = args[1];
		brokerID = new HashMap<>();
	
		// create and fill the hashmap
		readfile();
	
	}
	
	public void readfile(){
		try{
			output = new PrintWriter(new File("Results."+outputFilename));
			File file = new File(inputFilename); 
			Scanner sc = new Scanner(file); 
			String bName = "";
			String bDrPrice = "";
			String bDrMWh = "";
			String bCrPrice = "";
			String bCrMWh = "";
			
			// Start reading the file and CALCULATE AVG
			int ts = 0;
			double brokerTx$AllTS[] = new double [10];
			double brokerTxWAllTS[] = new double [10];
			
			double brokerDrAllTS[] = new double [10];
			double brokerCrAllTS[] = new double [10];
			double brokerDrDiffAllTS[] = new double [10];
			double brokerTxDrAllTS[] = new double [10];
			double brokerCrDiffAllTS[] = new double [10];
			double brokerTxCrAllTS[] = new double [10];
			
			double brokerDrUOneTS[] = new double [10];
			double brokerTxDrOneTS[] = new double [10];
			double brokerCrUOneTS[] = new double [10];
			double brokerTxCrOneTS[] = new double [10];
			
			while (sc.hasNextLine()){
				String line = sc.nextLine();
				if(!line.equalsIgnoreCase("")){
					String [] arrVals = line.split(",",-1);
					if(firstTime)
					{
						int length = arrVals.length;
						n = length/ITEMS;
						firstTime = false;
					}
					
					int index = 0;
					for(int i = 0; i < n; i++){

						// Get the broker name
						bName = arrVals[index];
						index++;
						String name = brokerID.get(i);
						if(name == null)
							brokerID.put(i, bName);
						
						double dr$ = 0.0;
						double drTx = 0.0;
						
						double cr$ = 0.0;
						double crTx = 0.0;
						
						for(int j=0;j<HA;j++)
						{
							double drpr = 0.0;
							double drmwh = 0.0;
							double crpr = 0.0;
							double crmwh = 0.0;
							
							// get price debit
							bDrPrice = arrVals[index];
							index++;
							if(!bDrPrice.equals("")){
								drpr = Double.parseDouble(bDrPrice);
							}

							// get Mwh debit    				
							bDrMWh = arrVals[index];
							index++;
							if(!bDrMWh.equals("")){
								drmwh = Double.parseDouble(bDrMWh);
							}
							
							// get price credit
							bCrPrice = arrVals[index];
							index++;
							if(!bCrPrice.equals("")){
								crpr = Double.parseDouble(bCrPrice);
							}

							// get Mwh credit    				
							bCrMWh = arrVals[index];
							index++;
							if(!bCrMWh.equals("")){
								crmwh = Double.parseDouble(bCrMWh);
							}
							
							if(j > 0){ // Restrict 0 HourAhead auctions
								dr$ = dr$ + (drpr*Math.abs(drmwh)); 
								cr$ = cr$ + (crpr*Math.abs(crmwh));
								drTx = drTx + drmwh; 
								crTx = crTx + crmwh;
							}
							
						} // Finished one timeslot for a broker
						
						double balDrV = 0;
						double balDrP = 0;
						double balCrV = 0;
						double balCrP = 0;
						
						String balDr = arrVals[index];
						index++;
						if(!balDr.equals("") && !balDr.equalsIgnoreCase("NaN")){
							balDrP = Double.parseDouble(balDr);
						}
						
						String balDrVol = arrVals[index];
						index++;
						if(!balDrVol.equals("") && !balDrVol.equalsIgnoreCase("NaN")){
							balDrV = Double.parseDouble(balDrVol);
						}
						
						String balCr = arrVals[index];
						index++;
						if(!balCr.equals("") && !balCr.equalsIgnoreCase("NaN")){
							balCrP = Double.parseDouble(balCr);
						}
						
						String balCrVol = arrVals[index];
						index++;
						if(!balCrVol.equals("") && !balCrVol.equalsIgnoreCase("NaN")){
							balCrV = Double.parseDouble(balCrVol);
						}
						
						dr$ = dr$ + (balDrP);
						cr$ = cr$ + (balCrP);
						drTx = drTx + balDrV;
						crTx = crTx + balCrV;
						
						brokerTx$AllTS[i] += dr$ + cr$;
						brokerTxWAllTS[i] += drTx + crTx;
						
						// Update the cost Dr
						brokerDrUOneTS[i] = 0; 
						if(drTx != 0){
							brokerDrUOneTS[i] = dr$ / Math.abs(drTx); 
							brokerTxDrOneTS[i] = drTx;
						}
						
						// Update the cost Cr
						brokerCrUOneTS[i] = 0; 
						if(crTx != 0){
							brokerCrUOneTS[i] = cr$ / Math.abs(crTx); 
							brokerTxCrOneTS[i] = crTx;
						}
					}
					
					// After one TS
					for(int l = 0; l < n; l++){
						brokerDrAllTS[l] += Math.abs(brokerTxDrOneTS[l])*brokerDrUOneTS[l];
						brokerCrAllTS[l] += Math.abs(brokerTxCrOneTS[l])*brokerCrUOneTS[l];
						
						brokerDrDiffAllTS[l] += (Math.abs(brokerTxDrOneTS[l])*brokerDrUOneTS[n-1]) - (Math.abs(brokerTxDrOneTS[l])*brokerDrUOneTS[l]);
						brokerTxDrAllTS[l] += brokerTxDrOneTS[l]; 

						brokerCrDiffAllTS[l] += (Math.abs(brokerTxCrOneTS[l])*brokerCrUOneTS[l]) - (Math.abs(brokerTxCrOneTS[l])*brokerCrUOneTS[n-1]);
						brokerTxCrAllTS[l] += brokerTxCrOneTS[l]; 
						
						// reset values for one ts
						brokerDrUOneTS[l] = 0;
						brokerTxDrOneTS[l] = 0;
						brokerCrUOneTS[l] = 0;
						brokerTxCrOneTS[l] = 0;
					}
					ts++;
				}
			} // End While
			System.out.println("UNIT COST COMPARISON");
			output.println("UNIT COST COMPARISON");
			for(int i =0; i<n; i++)
			{
				System.out.println(brokerID.get(i) + " unitDr(Sell$) " + brokerDrAllTS[i]/Math.abs(brokerTxDrAllTS[i]) + " Gain-$ (+ve) " + brokerDrAllTS[i] + " Sold-MW (-ve) " + brokerTxDrAllTS[i]);
				System.out.println(brokerID.get(i) + " unitCr(Buy$) " + brokerCrAllTS[i]/Math.abs(brokerTxCrAllTS[i]) + " Lose-$ (-ve) " + brokerCrAllTS[i] + " Bought-MW (+ve) " + brokerTxCrAllTS[i]);
			}
			
			System.out.println("For All Tx");
			for(int i = 0; i<n; i++) {
				System.out.println(brokerID.get(i) + " unit$(Tot$) " + brokerTx$AllTS[i]/Math.abs(brokerTxWAllTS[i]) + " Total-$ " + brokerTx$AllTS[i] + " Total-Trade-MW " + brokerTxWAllTS[i]);
			}
			
			System.out.println("\n\nHOW FUNNY!");
			for(int i = 0; i < n; i++){
				System.out.println(brokerID.get(i) + " unitDrDiffWithAvgB " + brokerDrDiffAllTS[i]/Math.abs(brokerTxDrAllTS[i]) + " Sell-diff " + brokerDrDiffAllTS[i] + " Sold-MW " + brokerTxDrAllTS[i]);
				output.println(brokerID.get(i) + ",Dr," + brokerDrDiffAllTS[i]/Math.abs(brokerTxDrAllTS[i]));
				System.out.println(brokerID.get(i) + " unitCrDiffWithAvg " + brokerCrDiffAllTS[i]/Math.abs(brokerTxDrAllTS[i]) + " Buy-diff " + brokerCrDiffAllTS[i] + " Bough-MW " + brokerTxCrAllTS[i]);
				output.println(brokerID.get(i) + ",Cr," + brokerCrDiffAllTS[i]/Math.abs(brokerTxDrAllTS[i]));
			}
			output.close();
		}
		catch(Exception ex){
			System.out.println(ex.getMessage());
			ex.printStackTrace();
		}
		
		
	}

	public void run(String[] args){
		inputFilename = args[0];
		outputFilename = args[1];
		
		brokerDr = new HashMap<String, Data>();

		try{
			output = new PrintWriter(new File("Results."+outputFilename));
			File file = new File(inputFilename); 
			Scanner sc = new Scanner(file); 
			String bName = "";
			String bPrice = "";
			
			String bMWh = "";
			
			// Start reading the file and CALCULATE AVG
			while (sc.hasNextLine()){
				String line = sc.nextLine();
				if(!line.equalsIgnoreCase("")){
					String [] arrVals = line.split(",",-1);
					//length = arrVals.length;
					if(firstTime)
					{
						initiateMaps(line);
						firstTime = false;
					}
					int index = 0;
					for(int i = 0; i < n; i++){

						// Get the broker name
						bName = arrVals[index];
						index++;
						Data data = brokerDr.get(bName);
						
						for(int j=0;j<24;j++)
						{
							double pr = 0.0;
							double mwh = 0.0;
							
							// get price
							bPrice = arrVals[index];
							index++;
							if(!bPrice.equals("")){
								pr = Double.parseDouble(bPrice);
								//data.drSum[j] += Double.parseDouble(bPrice);
								data.drCount[j]++;
							}

							// get Mwh    				
							bMWh = arrVals[index];
							index++;
							if(!bMWh.equals("")){
								mwh = Double.parseDouble(bMWh);
								//data.drVolSum[j] += Double.parseDouble(bMWh);
								data.drVolCount[j]++;
							}
							
							data.drSum[j] += pr*mwh;
							data.drVolSum[j] +=mwh;
						}
					}
					N++;
				}
			}

			double[] avgdemand = new double[24];
			double[] avgprc = new double[24];
			
			// Avg broker demand
			for(int i = 0; i < 24; i++){
				double pr = 0.0;
				double mwh = 0.0;
				for (Map.Entry<String, Data> entry : brokerDr.entrySet()){
					String brokerName = entry.getKey();
					if(brokerName.equals("AvgBroker"))
						continue;
					
					Data brokerData = entry.getValue();
					pr += brokerData.drSum[i];
					mwh += brokerData.drVolSum[i];
				}
				avgprc[i] = pr/mwh;
				avgdemand[i] = mwh;
			}
			
			// We have the SUM, COUNT and SAMPLE SIZE
			// Calculating average
			for (Map.Entry<String, Data> entry : brokerDr.entrySet()){
				bName = entry.getKey();
				Data d = entry.getValue();

				for(int i = 0; i < 24; i++){
					// CALCULATING THE AVG
					// Replacing Sum with Avg value
					if(d.drVolSum[i] == 0)
						d.drSum[i] = 0;
					else 
						d.drSum[i] /= d.drVolSum[i];
				}
			}
			
			
			
			/****************************************************/
			// Std dev calculation : Start reading the file again
			file = new File(inputFilename); 
			sc = new Scanner(file);
			// Start reading the file
			while (sc.hasNextLine()){
				String line = sc.nextLine();
				if(!line.equalsIgnoreCase("")){
					int index = 0;
					for(int i = 0; i < n; i++){
						String [] arrVals = line.split(",",-1);
						// Get the broker name
						bName = arrVals[index];
						index++;
						Data data = brokerDr.get(bName);
						
						for(int j=0;j<24;j++)
						{ 
							// get price
							bPrice = arrVals[index];
							index++;
							if(!bPrice.equals("")){
								double diff = (data.drSum[j] - Double.parseDouble(bPrice));
								diff = diff*diff;
								data.drSTD[i] = ((data.drSTD[i]*data.drSTDCount[j])+diff)/(data.drSTDCount[j]+1);
								data.drSTDCount[j]++;
							}

							// get Mwh    				
							bMWh = arrVals[index];
							index++;
							if(!bMWh.equals("")){
							    double diff = (data.drVolSum[j] - Double.parseDouble(bMWh));
								diff = diff*diff;
								data.drSTDVol[i] = ((data.drSTDVol[i]*data.drSTDVolCount[j])+diff)/(data.drSTDVolCount[j]+1);
								data.drSTDVolCount[j]++;
							}
							
						}
					}
				} 
			}// End While: Finished calculating variance
			
			/*********************************************************/
			// Now print results
			System.out.println("********************************");
			System.out.println("************UnitPrices**********");
			System.out.println("********************************");
			
			output.println("****************************************");
			output.println("*********UnitPrices Hour Ahead**********");
			output.println("****************************************");
			
			for (Map.Entry<String, Data> entry : brokerDr.entrySet()){
				// Output the avg values to a file for all brokers
				String brokerName = entry.getKey();
				Data brokerData = entry.getValue();
				if(brokerName.equals("AvgBroker"))
					continue;
				
				output.format("%s,", brokerName);
				System.out.printf("%s,", brokerName);
				
				// Loops for avg unit price hour ahead and Print
				for(int j = 0; j < 24; j++){
					output.format(",%.4f", brokerData.drSum[j]);
					System.out.printf(",%.4f", brokerData.drSum[j]);
				}
				// Loops for avg vol hour ahead and Print
				for(int j = 0; j < 24; j++){
					output.format(",%.4f", brokerData.drVolSum[j]);
					System.out.printf(",%.4f", brokerData.drVolSum[j]);
				}
				output.println();
				System.out.println();
			}
			
			output.format("AvgBroker,");
			System.out.printf("AvgBroker,");
			
			// Loops for avg unit price hour ahead and Print
			for(int j = 0; j < 24; j++){
				output.format(",%.4f", avgprc[j]);
				System.out.printf(",%.4f", avgprc[j]);
			}
			// Loops for avg vol hour ahead and Print
			for(int j = 0; j < 24; j++){
				output.format(",%.4f", avgdemand[j]);
				System.out.printf(",%.4f", avgdemand[j]);
			}
			output.println();
			System.out.println();
			
			System.out.println("********************************");
			System.out.println("************Cost**********");
			System.out.println("********************************");
			
			output.println("****************************************");
			output.println("*********Cost Comparison**********");
			output.println("****************************************");
			
			
			double[] spotdemand = brokerDr.get("SPOT").drVolSum;
			double totspotdemand = 0;
			for (Map.Entry<String, Data> entry : brokerDr.entrySet()){
				// Output the avg values to a file for all brokers
				String brokerName = entry.getKey();
				Data brokerData = entry.getValue();
				if(brokerName.equals("AvgBroker"))
					continue;
				
				output.format("%s,", brokerName);
				System.out.printf("%s,", brokerName);
				
				// Loops for avg unit price hour ahead and Print
				double cost = 0;
				for(int j = 1; j < 24; j++){
					if(brokerData.drSum[j] != 0){
						cost +=((avgprc[j]-brokerData.drSum[j])*spotdemand[j]);
					}
					else{
						System.out.println("0 price at HA " + j);
					}
					totspotdemand += spotdemand[j];
				}
				output.format(",%.4f\n", cost/totspotdemand);
				System.out.printf(",%.4f\n", cost/totspotdemand);
			}
			/*
			// double[] avgdemand = brokerDr.get("AvgBroker").drVolSum;
			
			// Calculate error for 95% confidence interval and PRINT RESULTS
			for (Map.Entry<String, Data> entry : brokerDr.entrySet()){
				String brokerName = entry.getKey();
				Data brokerData = entry.getValue();
				double errsum24 = 0.0;
				double errc24 = 0.0;
				double errsum23 = 0.0;
				double errc23 = 0;
				double sumcostSPOT24 = 0.0;
				double sumcostcSPOT24 = 0.0;
				double sumcostAVGB24 = 0.0;
				double sumcostcAVGB24 = 0.0;
				
				double sumcostSPOT23 = 0.0;
				double sumcostcSPOT23 = 0.0;
				double sumcostAVGB23 = 0.0;
				double sumcostcAVGB23 = 0.0;
				
				
				// With 24 hourAhead auctions
				for(int i=0;i<24;i++){
					double count = brokerData.drCount[i];
					double haavgprice = brokerData.drSum[i];
					double stdSum = brokerData.drSTD[i];
					double stdCount = brokerData.drSTDCount[i];
					double stdSumVol = brokerData.drSTDVol[i];
					double stdVolCount = brokerData.drSTDVolCount[i];
					if(count==0){
						System.out.println(brokerName + ": 0 prices at " + i + " HA replacing stdSum " + stdSum +" with " + brokerDr.get("AvgBroker").drSTD[i]);
						stdSum = brokerDr.get("AvgBroker").drSTD[i];
						stdCount = brokerDr.get("AvgBroker").drSTDCount[i];
						stdSumVol = brokerDr.get("AvgBroker").drSTDVol[i];
						stdVolCount = brokerDr.get("AvgBroker").drSTDVolCount[i];
						haavgprice = brokerDr.get("AvgBroker").drSum[i];
					}
					
					if(stdCount == 0){
						brokerData.drSTD[i] = 0;
						brokerData.drSTDVol[i] = 0;
						brokerData.drErr[i] = 0;
						brokerData.drErrVol[i] = 0;
					}
					else {
						brokerData.drSTD[i]=Math.sqrt(stdSum/stdCount);
						brokerData.drSTDVol[i]=Math.sqrt(stdSumVol/stdVolCount);
						brokerData.drErr[i] = 1.96*(brokerData.drSTD[i]/Math.sqrt(N));
						brokerData.drErrVol[i] = 1.96*(brokerData.drSTDVol[i]/Math.sqrt(N));
						errsum24 += brokerData.drErr[i];
						errc24++;
						if(i > 0){
							errsum23 += brokerData.drErr[i];
							errc23++;
							sumcostSPOT23 += (haavgprice*spotdemand[i]);
							sumcostcSPOT23+=spotdemand[i];
							sumcostAVGB23 += (haavgprice*avgdemand[i]);
							sumcostcAVGB23+=avgdemand[i];
						}
						sumcostSPOT24 += (haavgprice*spotdemand[i]);
						sumcostcSPOT24+=spotdemand[i];
						sumcostAVGB24 += (haavgprice*avgdemand[i]);
						sumcostcAVGB24+=avgdemand[i];
					}
				}
				
				if(errc24 > 0){
					brokerData.avgUnitError24 = errsum24/errc24;
					brokerData.avgUnitPriceSPOTD24 = sumcostSPOT24/sumcostcSPOT24;
					brokerData.avgUnitPriceAVGBD24 = sumcostAVGB24/sumcostcAVGB24;
					if(errc23 > 0){
						brokerData.avgUnitError23 = errsum23/errc23;
						brokerData.avgUnitPriceSPOTD23 = sumcostSPOT23/sumcostcSPOT23;
						brokerData.avgUnitPriceAVGBD23 = sumcostAVGB23/sumcostcAVGB23;
					}
				}
			}
			
			System.out.println("********************************");
			System.out.println("************UnitPrices**********");
			System.out.println("********************************");
			
			output.println("****************************************");
			output.println("*********UnitPrices Hour Ahead**********");
			output.println("****************************************");
			
			for (Map.Entry<String, Data> entry : brokerDr.entrySet()){
				// Output the avg values to a file for all brokers
				String brokerName = entry.getKey();
				output.format("%s,", brokerName);
				System.out.printf("%s,", brokerName);
				Data brokerData = entry.getValue();
				
				// Loops for avg unit price hour ahead and Print
				for(int j = 0; j < 24; j++){
					output.format(",%.4f", brokerData.drSum[j]);
					System.out.printf(",%.4f", brokerData.drSum[j]);
				}
				// Loops for avg vol hour ahead and Print
				for(int j = 0; j < 24; j++){
					output.format(",%.4f", brokerData.drVolSum[j]);
					System.out.printf(",%.4f", brokerData.drVolSum[j]);
				}
				output.println();
				System.out.println();
				output.format("%s Err,", brokerName);
				System.out.printf("%s Err,", brokerName);
				// Loop for error bound on hour ahead prices
				for(int j = 0; j < 24; j++){
					output.format(",%.4f", brokerData.drErr[j]);
					System.out.printf(",%.4f", brokerData.drErr[j]);
				}
				// // Loop for error bound on hour ahead demands
				for(int j = 0; j < 24; j++){
					output.format(",%.4f", brokerData.drErrVol[j]);
					System.out.printf(",%.4f", brokerData.drErrVol[j]);
				}
				output.println();
				System.out.println();
			}
			
			System.out.println("**********************************");
			System.out.println("************Final Result**********");
			System.out.println("**********************************");
			
			output.println("****************************************");
			output.println("*********Final Result*******************");
			output.println("****************************************");
			
			System.out.println("Broker, UtPr23SPOTD, UtPr24SPOTD, UtPr23AVGBD, UtPr24AVGBD, Err23, Err24");
			output.println("Broker, UtPr23SPOTD, UtPr24SPOTD, UtPr23AVGBD, UtPr24AVGBD, Err23, Err24");
			for (Map.Entry<String, Data> entry : brokerDr.entrySet()){
				// Output the avg values to a file for all brokers
				String brokerName = entry.getKey();
				Data d = entry.getValue();
				System.out.printf("%s,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f", brokerName, d.avgUnitPriceSPOTD23, d.avgUnitPriceSPOTD24, d.avgUnitPriceAVGBD23, d.avgUnitPriceAVGBD24, d.avgUnitError23, d.avgUnitError24);;
				output.format("%s,%.4f,%.4f,%.4f,%.4f,%.4f,%.4f", brokerName, d.avgUnitPriceSPOTD23, d.avgUnitPriceSPOTD24, d.avgUnitPriceAVGBD23, d.avgUnitPriceAVGBD24, d.avgUnitError23, d.avgUnitError24);
				output.println();
				System.out.println();
			}
			
			*/
			sc.close();
		}
		catch(Exception ex){
			System.out.println(ex.getMessage());
			ex.printStackTrace();
		}
		
		output.close();
	}

	public void initiateMaps(String line){
		String [] arrVals = line.split(",",-1);
		int length = arrVals.length;
		n = length/ITEMS;
		int index = 0;
		String bName = "";
		//System.out.println("n="+n+" Myline: "+ line);
		//System.out.println("1"+arrVals[0]+" 2 "+ arrVals[49]+ " 3 " + arrVals[49+49]);
		for(int i = 0; i < n; i++){
			bName = arrVals[index];
			//System.out.println("Broker "+i+" "+bName);
			brokerDr.put(bName, new Data());
			index+=ITEMS;
		}
	}
}

class Data2{
	public double [] prices;
	double demand;
	public Data2(int n){
		prices = new double[n];
	}
}

class Data{
	int ITEMS = 24;
	public double avgUnitPriceSPOTD23 =0;
	public double avgUnitPriceAVGBD23 =0;
	public double avgUnitPriceSPOTD24 =0;
	public double avgUnitPriceAVGBD24 =0;
	public double avgUnitError23;
	public double avgUnitError24;
	public double[] drSum;
	public double[] drCount;
	public double[] drVolSum;
	public double[] drVolCount;
	public double[] drSTD;
	public double[] drSTDCount;
	public double[] drSTDVol;
	public double[] drSTDVolCount;
	public double[] drErr;
	public double[] drErrVol;
	
	public double cost;
	public double volume;
	
	public Data(){
		drSum = new double[ITEMS];
		drCount = new double[ITEMS];
		drVolSum = new double[ITEMS];
		drVolCount = new double[ITEMS];
		drSTD = new double[ITEMS];
		drSTDCount = new double[ITEMS];
		drSTDVol = new double[ITEMS];
		drSTDVolCount = new double[ITEMS];
		drErr = new double[ITEMS];
		drErrVol = new double[ITEMS];
	}
}