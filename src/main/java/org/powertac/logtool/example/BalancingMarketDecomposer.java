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
import java.util.Map;
import java.util.TreeMap;



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
import org.powertac.common.CapacityTransaction;
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
public class BalancingMarketDecomposer extends LogtoolContext implements Analyzer {
//	static private Logger log = Logger.getLogger(MktPriceStats.class.getName());

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
	private double[] brokers = new double[12];
	private String[] brokernames = new String[12];
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


	/**
	 * Main method just creates an instance and passes command-line args to its
	 * inherited cli() method.
	 */
	public static void main(String[] args) {
		System.out.println("I am running");
		new BalancingMarketDecomposer().cli(args);
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
		//dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
		timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
		timeService = (TimeService) getBean("timeService");
		brokerRepo = (BrokerRepo) SpringApplicationContext
				.getBean("brokerRepo");
		//registerNewObjectListener(new TariffTransactionHandler(), TariffTransaction.class);
		//registerNewObjectListener(new CapacityTransactionHandler(), CapacityTransaction.class);
		registerNewObjectListener(new CompetitionHandler(), Competition.class);
		registerNewObjectListener(new BrokerHandler(), Broker.class);
		registerNewObjectListener(new TimeslotUpdateHandler(), TimeslotUpdate.class);
		//registerNewObjectListener(new MarketTransactionHandler(), MarketTransaction.class);
		registerNewObjectListener(new BalancingTransactionHandler(), BalancingTransaction.class);
		registerNewObjectListener(new TimeslotHandler(), Timeslot.class);
		//registerNewObjectListener(new WeatherReportHandler(), WeatherReport.class);
		//registerNewObjectListener(new OrderbookHandler(), Orderbook.class);
		//registerNewObjectListener(new WeatherForecastHandler(), WeatherForecast.class);
		//registerNewObjectListener(new DistributionTransactionHandler(), DistributionTransaction.class);
		//registerNewObjectListener(new CashPositionHandler(), CashPosition.class);
		//registerNewObjectListener(new BankTransactionHandler(), BankTransaction.class);


		ignoreCount = ignoreInitial;
		data = new TreeMap<Integer, ClearedTrade[]>();
		marketData = new TreeMap<Integer, SimulationDataPerTimeSlot>();
		try {
			//output = new PrintWriter(new File(dataFilename));
			FileWriter fw = new FileWriter(dataFilename, true);
			output = new PrintWriter(new BufferedWriter(fw));
			debug =  new PrintWriter(new File("debug.txt"));
		} catch (Exception e) {
//			log.error("Cannot open file " + dataFilename);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.powertac.logtool.ifc.Analyzer#report()
	 */
	@Override
	public void report() {
		
		double arroverallBalNet[] = new double[12];
		double arroverallBalKWHNet[] = new double[12];
		
		output.format("Timeslot, ");
		for(int i = 1; i <= numberofbrokers; i++)
			output.format(brokernames[i]+"-KWH, ");
		
		output.format("imbalance, ");
		
		for(int i = 1; i <= numberofbrokers; i++)
			output.format(brokernames[i]+"-$, ");
		output.format("\n");
		int ts = 360;
		for (Map.Entry<Integer, SimulationDataPerTimeSlot> entry : marketData
				.entrySet()) {

			SimulationDataPerTimeSlot trades = entry.getValue();

			output.format(ts + ", "); 
			double imbalance = 0.00;
			for(int i = 1; i <= numberofbrokers; i++){
				arroverallBalKWHNet[i] += trades.balancingKWH[i];
				imbalance += trades.balancingKWH[i];
				output.format(trades.balancingKWH[i] + ", ");
			}
			output.format(imbalance + ", "); 
			for(int i = 1; i <= numberofbrokers; i++){
				arroverallBalNet[i] += trades.balancing[i];
				output.format(trades.balancing[i] + ", ");
			}
			output.format("\n");
			ts++;
		}
		
		output.format("Broker, KWH, KWH, $, $ \n");
		for(int i = 1; i <= numberofbrokers; i++){
			output.format(brokernames[i] + ", KWH, " + "%.2f, $, %.2f\n", arroverallBalKWHNet[i], arroverallBalNet[i]);
		}
		output.close();
		debug.close();
		System.out.println("Finished");
	}

	// -----------------------------------
	// catch ClearedTrade messages
	class ClearedTradeHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			System.out.println("1");
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
//				log.error("ClearedTrade index error: " + offset);
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
			// Working System.out.println("2");
			Competition competition = (Competition) comp;
			BalancingMarketDecomposer.numberofbrokers = competition.getBrokers().size();
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
			// Working System.out.println("3");
			if (ignoreCount-- <= 0) {
				int timeslotSerial = timeslotRepo.currentSerialNumber();
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
			// Working System.out.println("4");
			Orderbook ob = (Orderbook) thing;

			if (ignoreCount-- <= 0) {
				//int currenttimeslot = timeslotRepo.currentSerialNumber();
				int timeslotSerial = ob.getTimeslotIndex();

				SimulationDataPerTimeSlot cmt = marketData.get(timeslotSerial);


				if (null == cmt) {
					cmt = new SimulationDataPerTimeSlot();

				}

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

	class WeatherReportHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			// Working System.out.println("5");
			WeatherReport wr = (WeatherReport) thing;

			//System.out.println("In the weather report handler");

			if (ignoreCount-- <= 0) {
				int currenttimeslot = timeslotRepo.currentSerialNumber();
				int timeslotSerial = wr.getTimeslotIndex();
				SimulationDataPerTimeSlot cmt = marketData.get(timeslotSerial);
				if (null == cmt) {
					cmt = new SimulationDataPerTimeSlot();
				}
				double temperature = wr.getTemperature();
				cmt.temp = temperature;

				double cloudcover = wr.getCloudCover();
				cmt.cloudCoverage = cloudcover;

				double windDir = wr.getWindDirection();
				cmt.windDirection = windDir;

				double windSpeed = wr.getWindSpeed();
				cmt.windSpeed = windSpeed;
				//System.out.println("temp : " + temperature);

				marketData.put(timeslotSerial, cmt);
			//	System.out.println("Currenttimeslot :" + currenttimeslot + " Weather Report for : " + timeslotSerial);
				debug.println("Currenttimeslot :" + currenttimeslot + " Weather Report for : " + timeslotSerial);

			}
		}
	}

	class WeatherForecastHandler implements NewObjectListener {

		public void handleNewObject(Object thing) {
			//Working System.out.println("6");
			WeatherForecast wf = (WeatherForecast) thing;
			//System.out.println("In the weather forecast Prediction handler");

			if (ignoreCount-- <= 0) {
				int advanceHour;
				int currenttimeslot = timeslotRepo.currentSerialNumber();
				int timeslotSerial = wf.getTimeslotIndex();

				SimulationDataPerTimeSlot cmt = marketData.get(timeslotSerial);
				if (null == cmt) {
					cmt = new SimulationDataPerTimeSlot();
				}

				for(int i = 0; i < wf.getPredictions().size(); i++)
				{
					WeatherForecastPrediction wfp = wf.getPredictions().get(i);
					advanceHour = wfp.getForecastTime();
					if(advanceHour > 0)
					{
					//	System.out.println("AdvanceHour" + advanceHour );
						cmt.wfTemp[advanceHour] = wfp.getTemperature();
						cmt.wfCloudCover[advanceHour] = wfp.getCloudCover();
						cmt.wfWindDir[advanceHour] = wfp.getWindDirection();
						cmt.wfWindSpeed[advanceHour] = wfp.getWindSpeed();
					//	System.out.println("CurrentTimeslot " + currenttimeslot + " Weather Forecast for : " + timeslotSerial +" hourAhead " + advanceHour);
						debug.println("CurrentTimeslot " + currenttimeslot + " Weather Forecast for : " + timeslotSerial +" hourAhead " + advanceHour);
					}
				}
 				marketData.put(timeslotSerial, cmt);

			}
		}
	}


	class CapacityTransactionHandler implements NewObjectListener {
		@Override
		public void handleNewObject(Object thing) {
			//System.out.println("Working ");
			if (ignoreCount > 0) {
				return;
			}
			CapacityTransaction ct = (CapacityTransaction) thing;
			int target = ct.getPostedTimeslot().getSerialNumber();
			SimulationDataPerTimeSlot cmt = marketData.get(target);
			if (null == cmt) {
				cmt = new SimulationDataPerTimeSlot();

			}
			if (ct.getBroker().getId() == brokerID) {
				cmt.capacityTransaction += ct.getCharge();
			}

			for(int i =0; i< brokers.length; i++){
				if((ct.getBroker().getId() == brokers[i])){
					cmt.arrCapacityTransaction[i] += ct.getCharge();
				}
			}
			marketData.put(target, cmt);

		}
	}



	// -----------------------------------
	// catch Broker events
	class BrokerHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			// Working System.out.println("7");
			Broker broker = (Broker) thing;
			String username = broker.getUsername().toUpperCase();
			if(username.equalsIgnoreCase("MISO") || username.equalsIgnoreCase("LMP")){
				// ignore that broker
			}
			else{
				System.out.print(broker.getUsername() + " ");
				brokerCounter++;
				if (username.equalsIgnoreCase("SPOT")) {
					brokerID = broker.getId();
				}
	
				//output.println(username + " ");
				//output.println();
				if (broker.getUsername().isEmpty()) {
					//output.println();
				}
	
				brokers[brokerCounter] = broker.getId();
				brokernames[brokerCounter] = username;
	
				System.out.println(brokers[brokerCounter] + " " + brokerCounter);
				numberofbrokers = brokerCounter;
			}
		}
	}

	public int getBrokerIndex(double brokerid){
		for(int i = 1; i <= numberofbrokers; i++){
			if(brokers[i] == brokerid)
				return i;
		}
		return -1;
	}
	
	class MarketTransactionHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			// Working System.out.println("8");
			if (ignoreCount > 0) {
				return; // nothing to do yet
			}
			MarketTransaction mt = (MarketTransaction) thing;
			int target = mt.getPostedTimeslot().getSerialNumber();
			SimulationDataPerTimeSlot cmt = marketData.get(target);

			if (null == cmt) {
				cmt = new SimulationDataPerTimeSlot();

			}
			double brokerid = mt.getBroker().getId();
			int brokerIndex = getBrokerIndex(brokerid);
			
			if(brokerIndex > 0){
				
				if((mt.getMWh() > 0 && mt.getPrice() > 0) || (mt.getMWh() < 0 && mt.getPrice() < 0))
					System.out.println("mt.getPrice() ->" + mt.getPrice() + " mt.getMWh() -> " + mt.getMWh());
					
				if (mt.getMWh() >= 0) {
					// bought energy
					cmt.arrenergyBought[brokerIndex] += Math.abs(mt.getMWh());
				} else {
					// sold energy
					cmt.arrenergySold[brokerIndex] += Math.abs(mt.getMWh());
				}
	
				// net calculation
				cmt.arrnetEnergy[brokerIndex] += mt.getMWh();
				cmt.arrnetPrice[brokerIndex] += (mt.getPrice() * Math.abs(mt.getMWh()));
				
				if ((mt.getPrice() * Math.abs(mt.getMWh())) >= 0) {
					cmt.arrmarketGain[brokerIndex] += (mt.getPrice() * Math.abs(mt.getMWh()));
				}
				else {
					cmt.arrmarketCost[brokerIndex] += ((mt.getPrice() * Math.abs(mt.getMWh())));
				}
	
				cmt.market[brokerIndex] += (mt.getPrice() * Math.abs(mt.getMWh()));
				
				marketData.put(target, cmt);
			}
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

	class DistributionTransactionHandler implements NewObjectListener {
		@Override
		public void handleNewObject(Object thing) {
			// Working System.out.println("10");
			//System.out.println("Calling DistributionTransactionHandler");
			if (ignoreCount > 0){
				return;
			}
			DistributionTransaction dt = (DistributionTransaction) thing;
			
			double brokerid = dt.getBroker().getId();
			int brokerIndex = getBrokerIndex(brokerid);
			
			int target = dt.getPostedTimeslot().getSerialNumber();
			SimulationDataPerTimeSlot cmt = marketData.get(target);
			if (null == cmt) {
				cmt = new SimulationDataPerTimeSlot();
			}
			if (dt.getBroker().getId() == brokerID) {
				cmt.netDistributionFee += dt.getCharge();
				//System.out.println(cmt.netDistributionFee);
				marketData.put(target, cmt);
			}
			
			if(brokerIndex > 0){
				cmt.distribution[brokerIndex] += dt.getCharge();
			}
			marketData.put(target, cmt);
		}
	}

	class TariffTransactionHandler implements NewObjectListener {
		@Override
		public void handleNewObject(Object thing) {
			//System.out.println("11");
			//System.out.println("Calling traiff transaction handler");
			if (ignoreCount > 0){
				return;
			}
			TariffTransaction tt = (TariffTransaction) thing;
			
			double brokerid = tt.getBroker().getId();
			int brokerIndex = getBrokerIndex(brokerid);
			
			int target = tt.getPostedTimeslot().getSerialNumber();
			SimulationDataPerTimeSlot cmt = marketData.get(target);
			if (null == cmt) {
				cmt = new SimulationDataPerTimeSlot();
			}
			if (tt.getBroker().getId() == brokerID){


				if (tt.getCharge() >= 0) {
					cmt.tariffGain += tt.getCharge();
				}
				else {
					cmt.tariffCost += tt.getCharge() * -1;
				}

				cmt.tariffNetPrice += tt.getCharge();
			}
			
			if((brokerIndex > 0)){
				cmt.tariff[brokerIndex] += tt.getCharge();
				//System.out.println("Tariff transaction : " + cmt.tariff[i] + " brokerid " + brokers[i] + " timeslot " + target);
			}
		
			marketData.put(target, cmt);
		}
	}

	class CashPositionHandler implements NewObjectListener {
		public void handleNewObject(Object thing){
			// Workring System.out.println("12");
			if (ignoreCount > 0){
				return;
			}
			CashPosition cp = (CashPosition) thing;
			
			double brokerid = cp.getBroker().getId();
			int brokerIndex = getBrokerIndex(brokerid);
			
			if(brokerIndex > 0){
				int target = cp.getPostedTimeslot().getSerialNumber();
				SimulationDataPerTimeSlot cmt = marketData.get(target);
				if (null == cmt) {
					cmt = new SimulationDataPerTimeSlot();
				}
				cmt.arrcashPosition[brokerIndex] = cp.getBalance();
				//System.out.println("Cashposition " + cp.getBalance() + " at timeslot " + target);
				marketData.put(target, cmt);
			}
		}
	}


	class BankTransactionHandler implements NewObjectListener {
		public void handleNewObject(Object thing){
			// Working System.out.println("13");
			if (ignoreCount > 0){
				return;
			}
			BankTransaction bt = (BankTransaction) thing;
			int target = bt.getPostedTimeslot().getSerialNumber();
			SimulationDataPerTimeSlot cmt = marketData.get(target);
			if (null == cmt) {
				cmt = new SimulationDataPerTimeSlot();
			}
			if (bt.getBroker().getId() == brokerID){

				cmt.bankNet = bt.getAmount();
				if (bt.getAmount() >= 0) {
					cmt.bankGain += bt.getAmount();
				}
				else {
					cmt.bankCost += bt.getAmount() * -1;
				}
				marketData.put(target, cmt);
			}

			for(int i =0; i< brokers.length; i++){
				if((bt.getBroker().getId() == brokers[i])){
					cmt.bank[i] += bt.getAmount();
				}
			}

		}
	}

	class TimeslotHandler implements NewObjectListener {

		public void handleNewObject(Object thing) {
			System.out.println("14");
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
