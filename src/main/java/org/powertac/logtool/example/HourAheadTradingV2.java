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
import java.util.TreeMap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.powertac.common.BalancingTransaction;
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
 * @author John Collins
 */
public class HourAheadTradingV2
extends LogtoolContext
implements Analyzer
{
	static private Logger log = LogManager.getLogger(MktPriceStats.class.getName());

	// service references
	private TimeslotRepo timeslotRepo;
	private TimeService timeService;
	// Data
	private List<Broker> brokerList;
	private ArrayList<String> brokerNamesSorted;
	private HashMap <String, Broker> brokerMap;

	private BrokerRepo brokerRepo;
	private TreeMap<Integer, ClearedTrade[]> data;
	private HashMap<Broker, HashMap<Integer, HashMap<Integer, ArrayList<MarketTransaction>>>> dataMtx;
	private HashMap<Broker, HashMap<Integer, BalancingTransaction>> dataBtx;
	private double totalImbalance;

	private int ignoreInitial = 5; // timeslots to ignore at the beginning
	private int ignoreCount = 0;
	private int indexOffset = 0; // should be Competition.deactivateTimeslotsAhead - 1

	private boolean omitHeaders = false;
	//private PrintWriter output = null;
	private PrintWriter outputCr = null;
	private PrintWriter outputDr = null;
	private String dataFilename = "clearedTrades.data";
	private double [] avgBrCr;
	private double [] avgBrCrVol;
	private double [] avgBrDr;
	private double [] avgBrDrVol;
	private double avgBal;
	private double avgBalVol;
	/**
	 * Main method just creates an instance and passes command-line args to
	 * its inherited cli() method.
	 */
	public static void main (String[] args)
	{
		new HourAheadTradingV2().cli(args);
	}

	/**
	 * Takes two args, input filename and output filename
	 */
	private void cli (String[] args)
	{
		if (args.length != 2) {
			System.out.println("Usage: <analyzer> [--no-headers] input-file output-file");
			return;
		}
		int argOffset = 0;
		if (args[0].equalsIgnoreCase("--no-headers")) {
			argOffset = 1;
			omitHeaders = true;
		}
		dataFilename = args[argOffset + 1];
		super.cli(args[argOffset], this);
	}

	/* (non-Javadoc)
	 * @see org.powertac.logtool.ifc.Analyzer#setup()
	 */
	@Override
	public void setup ()
	{
		brokerRepo = (BrokerRepo) getBean("brokerRepo");
		brokerList = new ArrayList<>();
		brokerNamesSorted = new ArrayList<>();
		brokerMap = new HashMap<>();
		timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
		timeService = (TimeService) getBean("timeService");
		registerNewObjectListener(new TimeslotUpdateHandler(),
				TimeslotUpdate.class);
		registerNewObjectListener(new BalancingTxHandler(),
				BalancingTransaction.class);
		registerNewObjectListener(new MarketTransactionHandler(),
				MarketTransaction.class);
		registerNewObjectListener(new SimStartHandler(),
				SimStart.class);

		ignoreCount = ignoreInitial;
		data = new TreeMap<Integer, ClearedTrade[]>();

		avgBrDr = new double[24];
		avgBrDrVol = new double[24];
		avgBrCr = new double[24];
		avgBrCrVol = new double[24];

		try {
			outputDr = new PrintWriter(new File(dataFilename+"Dr.csv"));
			outputCr = new PrintWriter(new File(dataFilename+"Cr.csv"));
		}
		catch (FileNotFoundException e) {
			log.error("Cannot open file " + dataFilename);
		}
	}

	/* (non-Javadoc)
	 * @see org.powertac.logtool.ifc.Analyzer#report()
	 */
	@Override
	public void report ()
	{

		for (Map.Entry<Integer, ClearedTrade[]> entry : data.entrySet()) {
			String delim = "";

			// Printing market clearing prices first
			int ts = entry.getKey();
			ClearedTrade[] trades = entry.getValue();
			if (trades.length != 24)
				log.error("short array " + trades.length);

			/*
			// print the avg broker values
			outputDr.format("MeanCT,");
			outputCr.format("MeanCT,");

			for (int i = 0; i < trades.length; i++) {
				if (null == trades[i]) {
					outputDr.print(delim);
					outputDr.print(delim);
					outputCr.print(delim);
					outputCr.print(delim);
				}
				else {
					printtofile(outputCr, delim, trades[i].getExecutionPrice());
					printtofile(outputCr, delim, trades[i].getExecutionMWh());
					printtofile(outputDr, delim, trades[i].getExecutionPrice());
					printtofile(outputDr, delim, trades[i].getExecutionMWh());
				}
				delim = ",";
			}
			 */
			boolean firstBroker = true;
			// Now print broker informations
			for (String brokerName: brokerNamesSorted){
				Broker broker = brokerMap.get(brokerName);
				dumpDataSimEnd(broker, ts, firstBroker);
				firstBroker = false;
				//output.println();
			}
			// print the avg broker values
			outputCr.format(",AvgBroker");
			outputDr.format(",AvgBroker");
			for(int k = 0; k < 24; k++)
			{
				if(avgBrDrVol[k] == 0)
					avgBrDr[k] = 0;
				else
					avgBrDr[k] /= avgBrDrVol[k];

				printVals(outputDr, avgBrDr[k]);
				printVals(outputDr, avgBrDrVol[k]);

				if(avgBrCrVol[k] == 0)
					avgBrCr[k] = 0;
				else
					avgBrCr[k] /= avgBrCrVol[k];
				printVals(outputCr, avgBrCr[k]);
				printVals(outputCr, avgBrCrVol[k]);
				
				// reset
				avgBrDr[k] = 0.0;
				avgBrDrVol[k] = 0.0;
				avgBrCr[k] = 0.0;
				avgBrCrVol[k] = 0.0;
			}
			// Bal Tx
			double avgBalP = 0;
			if(avgBalVol != 0)
				avgBalP = avgBal/avgBalVol;
			printVals(outputDr, avgBalP);
			printVals(outputDr, avgBalVol);
			printVals(outputDr, totalImbalance);
			//reset bal
			avgBal = 0;
			avgBalVol = 0;
			totalImbalance = 0;
			outputCr.println();
			outputDr.println();
		}
		outputCr.println();
		outputDr.println();

		outputCr.close();
		outputDr.close();

	}

	public void printtofile(PrintWriter o, String delim, Double val){
		o.format("%s%.4f", delim, val);
	}

	private void dumpDataSimEnd(Broker broker, int ts, boolean firstBroker){
		//System.out.println("ok1");
		HashMap<Integer, HashMap<Integer, ArrayList<MarketTransaction>>> tsmtx = dataMtx.get(broker);
		String deli = ",";
		if(firstBroker)
			deli ="";

		outputCr.format(deli+"%s",broker.getUsername());
		outputDr.format(deli+"%s",broker.getUsername());

		if(tsmtx == null)
		{
			printTSEmptyValsForBroker();
			
		}
		else {
			HashMap<Integer, ArrayList<MarketTransaction>> hamtx = tsmtx.get(ts);
	
			// Handle deferred market transactions for this timeslot
			double mtxD = 0.0;
			double mtxC = 0.0;
			double mtxDVol = 0.0;
			double mtxCVol = 0.0;
	
			if(hamtx == null)
				printTSEmptyValsForBroker();
			else
			{
				for(int offset=0; offset<=23; offset++){
					ArrayList<MarketTransaction> txList = hamtx.get(offset);
					if (null != txList) {
						for (MarketTransaction tx: txList) {
							double money = tx.getPrice();//Math.abs(tx.getMWh()) * tx.getPrice();
							if (money >= 0.0){
								mtxC = money;
								mtxCVol += Math.abs(tx.getMWh());
								avgBrCr[offset] += Math.abs(tx.getMWh()) * tx.getPrice();
								avgBrCrVol[offset] += Math.abs(tx.getMWh());
							}
							else{
								mtxD = money;
								mtxDVol += Math.abs(tx.getMWh());
								avgBrDr[offset] += Math.abs(tx.getMWh()) * tx.getPrice();
								avgBrDrVol[offset] += Math.abs(tx.getMWh());
							}
						}
					}
					// Print the hourAhead transactions
					printVals(outputDr, mtxD);
					printVals(outputDr, mtxDVol);
					printVals(outputCr, mtxC);
					printVals(outputCr, mtxCVol);
					// reset
					mtxD = 0;
					mtxC = 0;
					mtxDVol = 0;
					mtxCVol = 0;
				}
			}
		}
		//System.out.println("OK");
		// Now print balancing tx
		HashMap<Integer, BalancingTransaction> tsbtx = dataBtx.get(broker);
		if(tsbtx == null) {
			printVals(outputDr, 0);
			printVals(outputDr, 0);
		}
		else {
			BalancingTransaction btx = tsbtx.get(ts);
			if(btx != null) {
				double charge = btx.getCharge();
				double vol = btx.getKWh()/1000;
				avgBal += charge;
				avgBalVol += vol;
				totalImbalance += vol;
				printVals(outputDr, charge);
				printVals(outputDr, vol);
				//System.out.println(broker.getUsername() + " charge " + charge + " vol " + vol);
			}
			else
			{
				printVals(outputDr, 0);
				printVals(outputDr, 0);
			}
		}
	}

	public void printTSEmptyValsForBroker(){
		for(int offset=0; offset<=23; offset++){
			printVals(outputDr, 0);
			printVals(outputDr, 0);
			printVals(outputCr, 0);
			printVals(outputCr, 0);
		}
	}

	public void printVals(PrintWriter output, double vals){
		if(vals == 0)
			output.format(",");
		else
			output.format(",%.4f", vals);
	}

	// -----------------------------------
	// catch ClearedTrade messages
	class SimStartHandler implements NewObjectListener
	{
		@Override
		public void handleNewObject (Object thing)
		{
			System.out.println("Simulation Started");
			List<Broker> tbrokerList = new ArrayList<Broker>();
			tbrokerList = brokerRepo.findRetailBrokers();

			for(Broker b: tbrokerList){
				brokerList.add(b);
				brokerNamesSorted.add(b.getUsername());
			}

			Collections.sort(brokerNamesSorted);

			for(String s: brokerNamesSorted){
				for(Broker b:brokerList){
					if(b.getUsername().equals(s)){
						brokerMap.put(s, b);
						break;
					}
				}
			}

			dataMtx = new HashMap<>();
			dataBtx = new HashMap<>();
			for (Broker broker: brokerList) {
				dataMtx.put(broker, null);
				dataBtx.put(broker, null);
			}
		}
	}

	/*
	// -----------------------------------
	// catch ClearedTrade messages
	class ClearedTradeHandler implements NewObjectListener
	{

		@Override
		public void handleNewObject (Object thing)
		{
			if (ignoreCount > 0) {
				return; // nothing to do yet
			}
			ClearedTrade ct = (ClearedTrade) thing;
			int target = ct.getTimeslotIndex();
			int now = timeslotRepo.getTimeslotIndex(timeService.getCurrentTime());
			int offset = target - now - indexOffset;
			if (offset < 0 || offset > 23) {
				// problem
				log.error("ClearedTrade index error: " + offset);
			}
			else {
				ClearedTrade[] targetArray = data.get(target);
				if (null == targetArray) {
					targetArray = new ClearedTrade[24];
					data.put(target, targetArray);
				}
				targetArray[offset] = ct;
			}
		}
	}

	 */
	// -------------------------------
	// catch BalancingTransactions
	class BalancingTxHandler implements NewObjectListener
	{
		@Override
		public void handleNewObject (Object thing)
		{
			BalancingTransaction tx = (BalancingTransaction)thing;
			//System.out.println("TS "+ tx.getPostedTimeslotIndex() + " " + tx.getBroker().getUsername() + " Btx c: " + tx.getCharge() + " mwh: " + tx.getKWh());
			HashMap<Integer, BalancingTransaction> tempBtx = dataBtx.get(tx.getBroker());
			if(tempBtx == null)
				tempBtx = new HashMap<Integer, BalancingTransaction>();
			if(tx.getCharge() < 0) {
				tempBtx.put(tx.getPostedTimeslotIndex(), tx);
				dataBtx.put(tx.getBroker(), tempBtx);
			}
		} 
	}
	//-----------------------------------
	// catch ClearedTrade messages
	class MarketTransactionHandler implements NewObjectListener
	{

		@Override
		public void handleNewObject (Object thing)
		{
			if (ignoreCount > 0) {
				return; // nothing to do yet
			}
			MarketTransaction mtx = (MarketTransaction) thing;

			int now = timeslotRepo.getTimeslotIndex(timeService.getCurrentTime());
			int target = mtx.getTimeslotIndex();
			int offset = target - now - indexOffset;
			
			Broker broker = mtx.getBroker();
			if (!brokerList.contains(broker)){
				//System.out.println(broker.getUsername() + " not in the list.");
				return;
			}

			HashMap<Integer, HashMap<Integer, ArrayList<MarketTransaction>>> tsmtx = dataMtx.get(broker);

			if(tsmtx == null){
				tsmtx = new HashMap<Integer, HashMap<Integer, ArrayList<MarketTransaction>>>();
				HashMap<Integer, ArrayList<MarketTransaction>> hamtx = new HashMap<Integer, ArrayList<MarketTransaction>>();
				ArrayList<MarketTransaction> arrmtx = new ArrayList<MarketTransaction>();
				arrmtx.add(mtx);
				hamtx.put(offset,arrmtx);
				tsmtx.put(target, hamtx);
				dataMtx.put(broker,tsmtx);
				return;
			}

			HashMap<Integer, ArrayList<MarketTransaction>> hamtx = tsmtx.get(target);

			if (offset < 0 || offset > 23) {
				// problem
				System.out.println("ClearedTrade index error: " + offset);
			}
			else {
				if(hamtx == null){
					hamtx = new HashMap<Integer, ArrayList<MarketTransaction>>();
					ArrayList<MarketTransaction> arrmtx = new ArrayList<MarketTransaction>();
					arrmtx.add(mtx);
					hamtx.put(offset,arrmtx);
					tsmtx.put(target, hamtx);
					return;
				}

				ArrayList<MarketTransaction> arrmtx = hamtx.get(offset);
				if (null == arrmtx) {
					arrmtx = new ArrayList<MarketTransaction>();
				}
				arrmtx.add(mtx);
				hamtx.put(offset, arrmtx);
				//System.out.println("Mtx added for "+broker.getUsername());
			}
		}
	}

	// -----------------------------------
	// catch TimeslotUpdate events
	class TimeslotUpdateHandler implements NewObjectListener
	{

		@Override
		public void handleNewObject (Object thing)
		{
			if (ignoreCount-- <= 0) {
				int timeslotSerial = timeslotRepo.currentSerialNumber();
				if (null == data.get(timeslotSerial)) {
					data.put(timeslotSerial, new ClearedTrade[24]);
				}
			}
		}
	}
}
