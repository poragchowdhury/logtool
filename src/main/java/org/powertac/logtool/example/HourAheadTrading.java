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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
 * @author John Collins
 */
public class HourAheadTrading
extends LogtoolContext
implements Analyzer
{
	static private Logger log = LogManager.getLogger(MktPriceStats.class.getName());

	// service references
	private TimeslotRepo timeslotRepo;
	private TimeService timeService;

	// Data
	private List<Broker> brokerList;
	private BrokerRepo brokerRepo;
	private TreeMap<Integer, ClearedTrade[]> data;
	private HashMap<Broker, HashMap<Integer, HashMap<Integer, ArrayList<MarketTransaction>>>> dataMtx;

	private int ignoreInitial = 5; // timeslots to ignore at the beginning
	private int ignoreCount = 0;
	private int indexOffset = 0; // should be Competition.deactivateTimeslotsAhead - 1

	private boolean omitHeaders = false;
	//private PrintWriter output = null;
	private PrintWriter outputCr = null;
	private PrintWriter outputDr = null;
	private PrintWriter outputCrVol = null;
	private PrintWriter outputDrVol = null;
	private String dataFilename = "clearedTrades.data";

	/**
	 * Main method just creates an instance and passes command-line args to
	 * its inherited cli() method.
	 */
	public static void main (String[] args)
	{
		new HourAheadTrading().cli(args);
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
		
		timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
		timeService = (TimeService) getBean("timeService");
		registerNewObjectListener(new TimeslotUpdateHandler(),
				TimeslotUpdate.class);
		registerNewObjectListener(new ClearedTradeHandler(),
				ClearedTrade.class);
		registerNewObjectListener(new MarketTransactionHandler(),
				MarketTransaction.class);
		registerNewObjectListener(new SimStartHandler(),
				SimStart.class);
		
		ignoreCount = ignoreInitial;
		data = new TreeMap<Integer, ClearedTrade[]>();

		try {
			outputCr = new PrintWriter(new File("Cr."+dataFilename));
			outputDr = new PrintWriter(new File("Dr."+dataFilename));
			outputCrVol = new PrintWriter(new File("CrVol."+dataFilename));
			outputDrVol = new PrintWriter(new File("DrVol."+dataFilename));
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
			for (int i = 0; i < trades.length; i++) {
				if (null == trades[i]) {
					outputCr.print(delim);
					outputCrVol.print(delim);
					outputDr.print(delim);
					outputDrVol.print(delim);
				}
				else {
					printtofile(outputCr, delim, trades[i].getExecutionPrice());
					printtofile(outputDr, delim, trades[i].getExecutionPrice());
					printtofile(outputCrVol, delim, trades[i].getExecutionMWh());
					printtofile(outputDrVol, delim, trades[i].getExecutionMWh());
				}
				delim = ",";
			}
			
			// Now print broker informations
			for (Broker broker: brokerList){
				dumpDataSimEnd(broker, ts);
				//output.println();
			}
			outputCr.println();
			outputCrVol.println();
			outputDr.println();
			outputDrVol.println();
		}
		outputCr.close();
		outputCrVol.close();
		outputDr.close();
		outputDrVol.close();
	}

	public void printtofile(PrintWriter o, String delim, Double val){
		o.format("%s%.4f", delim, val);
	}
	
	private void dumpDataSimEnd(Broker broker, int ts){
		
		HashMap<Integer, HashMap<Integer, ArrayList<MarketTransaction>>> tsmtx = dataMtx.get(broker);
		outputCr.format(",%s",broker.getUsername());
		outputCrVol.format(",%s",broker.getUsername());
		outputDr.format(",%s",broker.getUsername());
		outputDrVol.format(",%s",broker.getUsername());
		if(tsmtx == null)
		{
			printTSEmptyValsForBroker();
			return;
		}
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
						}
						else{
							mtxD = money;
							mtxDVol += Math.abs(tx.getMWh());
						}
					}
				}
				// Print the hourAhead transactions
				printVals(outputDr, mtxD);
				printVals(outputDrVol, mtxDVol);
				printVals(outputCr, mtxC);
				printVals(outputCrVol, mtxCVol);
				// reset
				mtxD = 0;
				mtxC = 0;
				mtxDVol = 0;
				mtxCVol = 0;
			}
		}
	}
	
	public void printTSEmptyValsForBroker(){
		for(int offset=0; offset<=23; offset++){
			printVals(outputDr, 0);
			printVals(outputDrVol, 0);
			printVals(outputCr, 0);
			printVals(outputCrVol, 0);
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
			brokerList = brokerRepo.findRetailBrokers();
			dataMtx = new HashMap<>();
			for (Broker broker: brokerList) {
				dataMtx.put(broker, null);
			}
		}
	}
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
			//System.out.println("target "+ target + " now " + now + " offset " + offset + " posted " + posted);
			
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
