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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
//import org.apache.commons.lang.StringUtils;
//import org.apache.log4j.Logger;
import org.joda.time.Instant;
import org.powertac.common.Broker;
import org.powertac.common.ClearedTrade;
import org.powertac.common.Competition;
import org.powertac.common.MarketTransaction;
import org.powertac.common.Orderbook;
import org.powertac.common.TimeService;
import org.powertac.common.WeatherForecast;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.repo.OrderbookRepo;
import org.powertac.common.repo.TimeslotRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.WeatherReport;
import org.powertac.common.Timeslot;
import org.powertac.common.WeatherForecastPrediction;
import org.powertac.common.DistributionTransaction;
import org.powertac.common.TariffTransaction;
import org.powertac.common.CashPosition;
import org.powertac.common.BankTransaction;



/**
 * Logtool Analyzer that reads ClearedTrade instances as they arrive and builds
 * an array for each timeslot giving all the market clearings for that
 * timeslot,s indexed by leadtime. The output data file has one line/timeslot
 * formatted as<br>
 * [mwh price] [mwh price] ...<br>
 * Each line has 24 entries, assuming that each timeslot is open for trading 24
 * times.
 * 
 * Usage: MktPriceStats state-log-filename output-data-filename
 * 
 * @author John Collins
 */
public class WholesaleMarketStats extends LogtoolContext implements Analyzer {
	//static private Logger log = Logger.getLogger(WholesaleMarketStats.class.getName());

	// service references
	private TimeslotRepo timeslotRepo;
	private TimeService timeService;
	private OrderbookRepo OrderbookRepo;
	private DomainObjectReader dor;
	private Timeslot timeslot;
	private BrokerRepo brokerRepo;

	// Data
	private TreeMap<Integer, ClearedTrade[]> data;
	private TreeMap<Integer, SimulationDataPerTimeSlot> marketData;
	TreeMap<Integer, Integer> orderbookCounter = new TreeMap<Integer, Integer>();
	private int counter = 0;
	private ArrayList<Long> brokers = new ArrayList<Long>();
	//private long[] brokers = new long[10];
	int brokerCounter = 0;
	
	
	private int ignoreInitial = 0; // timeslots to ignore at the beginning
	private int ignoreCount = 0;
	private int indexOffset = 0; // should be
									// Competition.deactivateTimeslotsAhead - 1
	public static int numberofbrokers = 0;
	private PrintWriter output = null;
	private PrintWriter debug = null;
	private String dataFilename = "clearedTrades.arff";
	public double brokerID;
	public ArrayList<String> brokernames = new ArrayList<String>();

	/**
	 * Main method just creates an instance and passes command-line args to its
	 * inherited cli() method.
	 */
	public static void main(String[] args) {
		System.out.println("I am running");
		new WholesaleMarketStats().cli(args);
	}

	/**
	 * Takes two args, input filename and output filename
	 */
	private void cli(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage: <analyzer> input-file output-file");
			return;
		}
		dataFilename = args[1];
		super.cli(args[0], this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.powertac.logtool.ifc.Analyzer#setup()
	 */
	@Override
	public void setup() {
		timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
		timeService = (TimeService) getBean("timeService");
		brokerRepo = (BrokerRepo) SpringApplicationContext
				.getBean("brokerRepo");
		registerNewObjectListener(new CompetitionHandler(), Competition.class);
		registerNewObjectListener(new BrokerHandler(), Broker.class);
		registerNewObjectListener(new TimeslotUpdateHandler(), TimeslotUpdate.class);
		registerNewObjectListener(new MarketTransactionHandler(), MarketTransaction.class);
		//registerNewObjectListener(new BalancingTransactionHandler(), BalancingTransaction.class);
		registerNewObjectListener(new TimeslotHandler(), Timeslot.class);
		//registerNewObjectListener(new OrderbookHandler(), Orderbook.class);		
		
		ignoreCount = ignoreInitial;
		data = new TreeMap<Integer, ClearedTrade[]>();
		marketData = new TreeMap<Integer, SimulationDataPerTimeSlot>();
		try {
			//output = new PrintWriter(new File(dataFilename));
			FileWriter fw = new FileWriter(dataFilename, true);
			output = new PrintWriter(new BufferedWriter(fw));
			debug =  new PrintWriter(new File("debug.txt"));
		} catch (Exception e) {
			//log.error("Cannot open file " + dataFilename);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.powertac.logtool.ifc.Analyzer#report()
	 */
	@Override
	public void report() {
		double averageClearingPrice[] = new double[24];
		double yesterdayAvg[] = new double[24];
		double previousdayAvg = 0.0;
		double overallNet = 0.0;
		double overallMktNet = 0.0;
		double overallBalNet = 0.0;
		double overallDistNet = 0.0;
		double overallTariffNet = 0.0;
		double overallBankNet = 0.0;
		double overallGain = 0.0;
		double overallCost = 0.0;
		
		double sumMarketUnitPrice = 0;
		double sumMarketGain = 0;
		double sumMarketCost = 0;
		double countTimeslots = 359;

		double wholesaleEngeryBought[] = new double[brokers.size()];
		double wholesaleEngerySold[] = new double[brokers.size()];
		double wholesaleEngeryBoughtPrice[] = new double[brokers.size()];
		double wholesaleEngerySoldPrice[] = new double[brokers.size()];
		double wholesaleEngeryGross[] = new double[brokers.size()];
		double wholesaleUnitPriceBought[] =  new double[brokers.size()];
		double wholesaleUnitPriceSold[] =  new double[brokers.size()];
		
		for(int j = 0; j < brokers.size() ; j++){
			output.print(brokernames.get(j)+",");
		}
		output.println();
		
		for (Map.Entry<Integer, SimulationDataPerTimeSlot> entry : marketData
				.entrySet()) {
			countTimeslots++;
			String delim = "";
			Integer timeslot = entry.getKey();
			SimulationDataPerTimeSlot trades = entry.getValue();
			
			//overallMktNet += trades.netPrice;
			//overallBalNet += trades.netPriceB;
			
			if (null == trades) {
				output.print(delim + "[0.0 0.0]");
			} 
			else 
			{
				for(int j = 0; j < brokers.size() ; j++){
					if(trades.arrTradeCountBuy[j] == 0)
						trades.arrTradeCountBuy[j] = 1;
					if(trades.arrTradeCountSell[j] == 0)
						trades.arrTradeCountSell[j] = 1;
					output.format("%.2f,", trades.arrMarketBuy[j]/trades.arrTradeCountBuy[j]);
					wholesaleEngeryBought[j] += Math.abs(trades.arrMarketBuyMWh[j]);
					wholesaleEngerySold[j] += Math.abs(trades.arrMarketSellMWh[j]);
					wholesaleEngeryBoughtPrice[j] += trades.arrMarketBuy[j];
					wholesaleEngerySoldPrice[j] += trades.arrMarketSell[j];
					wholesaleUnitPriceBought[j] += trades.arrMarketBuy[j]/trades.arrTradeCountBuy[j]; 
					wholesaleUnitPriceSold[j] += trades.arrMarketSell[j]/trades.arrTradeCountSell[j];		
				}
				output.println();
			} 
		}
		
		output.format("brokername,totalMWhBought,totalMWhSold,totalMoneySpent,totalMoneyGain,netBalance,unitBuyPrice,unitSellPrice");
		output.println();
		for(int j = 0; j < brokers.size() ; j++){
			double totalMoneySpent = wholesaleEngeryBoughtPrice[j] * wholesaleEngeryBought[j];
			double totalMoneyGain = wholesaleEngerySoldPrice[j] * wholesaleEngerySold[j];
			double netBalance = totalMoneyGain - totalMoneySpent;
			output.format("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f", brokernames.get(j), wholesaleEngeryBought[j], wholesaleEngerySold[j], 
					totalMoneySpent, totalMoneyGain, netBalance, wholesaleUnitPriceBought[j]/countTimeslots, wholesaleUnitPriceSold[j]/countTimeslots);
			output.println();
		}
		
		output.close(); 
		debug.close();
	}

	// -----------------------------------
	// catch ClearedTrade messages
	class ClearedTradeHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			if (ignoreCount > 0) {
				return; // nothing to do yet
			}
			ClearedTrade ct = (ClearedTrade) thing;
			int target = ct.getTimeslot().getSerialNumber();
			int now = timeslotRepo.getTimeslotIndex(timeService
					.getCurrentTime());
			int offset = target - now - indexOffset;
			if (offset < 0 || offset > 23) {
				// problem
				//log.error("ClearedTrade index error: " + offset);
			} else {
				ClearedTrade[] targetArray = data.get(target);
				if (null == targetArray) {
					targetArray = new ClearedTrade[24];
					data.put(target, targetArray);
				}
				targetArray[offset] = ct;
			}
		}
	}

	class CompetitionHandler implements NewObjectListener{
		@Override
		public void handleNewObject(Object comp){
			Competition competition = (Competition) comp;
			WholesaleMarketStats.numberofbrokers = competition.getBrokers().size();
			System.out.println("Number of brokers : " + competition.getBrokers().size() + " Simulation " + competition.toString());
			
		}
	}

	// -----------------------------------
	// catch MarketTransaction messages
	
	// -----------------------------------
	// catch TimeslotUpdate events
	class TimeslotUpdateHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			// SimulationDataPerTimeSlot cmt;
						
			if (ignoreCount-- <= 0) {
				int timeslotSerial = timeslotRepo.currentSerialNumber();	
				System.out.println("TimeslotUpdateHandler " + timeslotSerial);
				SimulationDataPerTimeSlot cmt = marketData.get(timeslotSerial);
				int dayOfWeek = timeslotRepo.currentTimeslot().dayOfWeek();
				int dayHour = timeslotRepo.currentTimeslot().slotInDay();
				counter = 0;
				if (null == cmt) {
					cmt = new SimulationDataPerTimeSlot();
				}
				cmt.day_date = timeslotRepo.currentTimeslot().getStartTime().getDayOfMonth();
				cmt.month_date = timeslotRepo.currentTimeslot().getStartTime().getMonthOfYear();
				cmt.day = dayOfWeek;
				cmt.hour = dayHour;
				// System.out.println("Got day of week : " + dayOfWeek);
				marketData.put(timeslotSerial, cmt);

			}
		}
	}

	// --s---------------------------------
	// catch OrderbookHandler events
	class OrderbookHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			Orderbook ob = (Orderbook) thing;

			if (ignoreCount-- <= 0) {
				//int currenttimeslot = timeslotRepo.currentSerialNumber();
				int timeslotSerial = ob.getTimeslotIndex();
				
				SimulationDataPerTimeSlot cmt = marketData.get(timeslotSerial);
				
				
				if (null == cmt) {
					cmt = new SimulationDataPerTimeSlot();
					
				}

				// System.out.println("In the orderbook");

				//System.out.println("Orderbook timeslotSerial : " + timeslotSerial);
				// System.out.println("Clearingprice : " +
				// ob.getClearingPrice());
				if (ob.getClearingPrice() == null) {
					cmt.lastclearingPrice = 0;
					
				} else {
					cmt.lastclearingPrice = ob.getClearingPrice();
					}
				
				if (ob.getClearingPrice() == null) {
					cmt.arrClearingPrices[counter] = 0;
					
				} else {
					cmt.arrClearingPrices[counter] = ob.getClearingPrice();
					}
				
				counter++;
				// System.out.println("Clearingprice : " + cmt.clearingPrice);
				orderbookCounter.put(timeslotSerial, counter);
				
				//System.out.println(counter);
				//System.out.println(timeslotSerial + ob.getClearingPrice());
				marketData.put(timeslotSerial, cmt);
				
			}
		}
	}

	// -----------------------------------
	// catch Broker events
	class BrokerHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			Broker broker = (Broker) thing;
			String username = broker.getUsername().toUpperCase();
			
			if (broker.getUsername().isEmpty()) {
				output.println();
			}
			brokernames.add(username);
			brokers.add(broker.getId());
			if (StringUtils.substring(username, 0, 4).equals("SPOT")){
				brokerID = broker.getId();
				System.out.println("Brokder Id of username " + username + brokerID);
			}
			//brokers[brokerCounter] = broker.getId();
			System.out.println(broker.getId());
			brokerCounter++;
		}
	}
	
	class MarketTransactionHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			if (ignoreCount > 0) {
				return; // nothing to do yet
			}
			MarketTransaction mt = (MarketTransaction) thing;
			int target = mt.getPostedTimeslot().getSerialNumber();
			SimulationDataPerTimeSlot cmt = marketData.get(target);
			
			if (null == cmt) {
				cmt = new SimulationDataPerTimeSlot();
				
			}
			if (mt.getBroker().getId() == brokerID) {
				if (mt.getMWh() >= 0) {
					// bought energy
					cmt.energyBought += mt.getMWh();
				} else {
					// sold energy
					cmt.energySold += mt.getMWh();
				}

				if (mt.getPrice() >= 0) {
					// sold price i.e. deposited into brokers account
					cmt.soldprice += mt.getPrice();
				} else {
					// bought price i.e. paid from brokers account
					cmt.boughtprice += mt.getPrice();
				}
				
				// net calculation
				cmt.netEnergy += mt.getMWh();
				cmt.netPrice += (mt.getPrice() * Math.abs(mt.getMWh()));
				if ((mt.getPrice() * Math.abs(mt.getMWh())) >= 0) {
					cmt.marketGain += (mt.getPrice() * Math.abs(mt.getMWh()));
				}
				else {
					cmt.marketCost += ((mt.getPrice() * Math.abs(mt.getMWh())));
				}

				cmt.timeslotIndex = target;
				// trade count
				cmt.count += 1;
			}
			for(int i =0; i< brokers.size(); i++){ 
				if((mt.getBroker().getId() == brokers.get(i))){
					if(mt.getPrice() < 0){
						// buying energy
						cmt.arrMarketBuy[i] += Math.abs(mt.getPrice());
						cmt.arrMarketBuyMWh[i] += Math.abs(mt.getMWh());
						cmt.arrTradeCountBuy[i]++;
					}
					else{
						// selling energy
						cmt.arrMarketSell[i] += Math.abs(mt.getPrice());
						cmt.arrMarketSellMWh[i] += Math.abs(mt.getMWh());
						cmt.arrTradeCountSell[i]++;
					}
					cmt.arrTradeCount[i]++;
					break;
				}
			}
			marketData.put(target, cmt);
		}
	}


	class BalancingTransactionHandler implements NewObjectListener {
		@Override
		public void handleNewObject(Object thing) {
			// Working System.out.println("9");
			if (ignoreCount > 0) {
				return;
			}
			
			BalancingTransaction bt = (BalancingTransaction) thing;
			
			double brokerid = bt.getBroker().getId();
			int brokerIndex = getBrokerIndex(brokerid);
			
			int target = bt.getPostedTimeslot().getSerialNumber();
			SimulationDataPerTimeSlot cmt = marketData.get(target);
			
			if (null == cmt) {
				cmt = new SimulationDataPerTimeSlot();
			}
			if (brokerIndex > 0) {
				if (bt.getCharge() > 0) {
					cmt.soldPriceB[brokerIndex] += bt.getCharge();
				}
				else {
					cmt.boughtPriceB[brokerIndex] += bt.getCharge();
				}
				cmt.balancing[brokerIndex] += bt.getCharge();
				cmt.balancingKWH[brokerIndex] += bt.getKWh();
				if (bt.getKWh() >= 0) {
					cmt.balEngSurpls[brokerIndex] += bt.getCharge();
				}
				else {
					cmt.balEngDefct[brokerIndex] += bt.getCharge() * -1;
				}
			}
			
			marketData.put(target, cmt);

		}
	}
	public int getBrokerIndex(double brokerid){
		for(int i = 1; i <= numberofbrokers; i++){
			//if(brokers[i] == brokerid)
			//	return i;
		}
		return -1;
	}
			
	class TimeslotHandler implements NewObjectListener {

		public void handleNewObject(Object thing) {
			System.out.println("TimeslotHandler");
			if (ignoreCount > 0) {
				return;
			}
			Timeslot ts = (Timeslot) thing;
			int target = ts.getSerialNumber();
			SimulationDataPerTimeSlot cmt = marketData.get(target);
			if (null == cmt) {
				cmt = new SimulationDataPerTimeSlot();

			}
			marketData.put(target, cmt);
		}
	}

}