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
import org.powertac.logtool.example.LogExtractor.WeatherReportHandler;
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
public class BalancingMarketTrainingDatasetGenerator extends LogtoolContext implements Analyzer {
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
		new BalancingMarketTrainingDatasetGenerator().cli(args);
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
		registerNewObjectListener(new BalancingTransactionHandler(), BalancingTransaction.class);
		registerNewObjectListener(new TimeslotHandler(), Timeslot.class);
		registerNewObjectListener(new WeatherReportHandler(), WeatherReport.class);

		try {
			//output = new PrintWriter(new File(dataFilename));
			FileWriter fw = new FileWriter(dataFilename, true);
			output = new PrintWriter(new BufferedWriter(fw));
			debug =  new PrintWriter(new File("debug.txt"));
		} catch (Exception e) {
			System.out.println("Cannot open file " + dataFilename);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see org.powertac.logtool.ifc.Analyzer#report()
	 */
	@Override
	public void report() {
		output.close();
		debug.close();
		System.out.println("Finished");
	}

		class CompetitionHandler implements NewObjectListener{
		@Override
		public void handleNewObject(Object comp){
			// Working System.out.println("2");
			Competition competition = (Competition) comp;
			BalancingMarketTrainingDatasetGenerator.numberofbrokers = competition.getBrokers().size();
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
				int dayOfWeek = timeslotRepo.currentTimeslot().dayOfWeek();
				int dayHour = timeslotRepo.currentTimeslot().slotInDay();
				counter = 0;
			}
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
			
			if (brokerIndex > 0 && !brokernames[brokerIndex].equalsIgnoreCase("MISO")&&!brokernames[brokerIndex].equalsIgnoreCase("LMP")) {
				output.format((bt.getKWh()/1000) + "," + (bt.getCharge()*1000)/Math.abs(bt.getKWh()) + "\n"); 
			}
		}
	}

	class WeatherReportHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {

			WeatherReport wr = (WeatherReport) thing;
			
			//System.out.println("In the weather report handler");

			if (ignoreCount-- <= 0) {
				int currenttimeslot = timeslotRepo.currentSerialNumber();
				int timeslotSerial = wr.getTimeslotIndex();
				
				//DataPerTimeSlot cmt = marketData.get(timeslotSerial);
				//if (null == cmt) {
				//	cmt = new DataPerTimeSlot();
				//}
				double temperature = wr.getTemperature();
				//cmt.temp = temperature;

				double cloudcover = wr.getCloudCover();
				//cmt.cloudCoverage = cloudcover;

				double windDir = wr.getWindDirection();
				//cmt.windDirection = windDir;

				double windSpeed = wr.getWindSpeed();
				//cmt.windSpeed = windSpeed;
				//System.out.println("temp : " + temperature);

				//marketData.put(timeslotSerial, cmt);
				//System.out.print("WeatherReportHandler : Currenttimeslot :" + currenttimeslot + " Weather Report for : " + timeslotSerial);
				//debug.println("Currenttimeslot :" + currenttimeslot + " Weather Report for : " + timeslotSerial);
				output.format(numberofbrokers + " nbroker," + temperature + " t," + cloudcover + " cc," + windDir + " wd," + windSpeed + " ws,");
			}
		}
	}
	
	class TimeslotHandler implements NewObjectListener {

		public void handleNewObject(Object thing) {
			System.out.println("14");
			if (ignoreCount > 0) {
				return;
			}
		}
	}

}
