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
import java.util.Map.Entry;
import java.util.TreeMap;





//import org.apache.log4j.Logger;
import org.joda.time.Instant;
import org.powertac.common.Broker;
import org.powertac.common.ClearedTrade;
import org.powertac.common.Competition;
import org.powertac.common.MarketTransaction;
import org.powertac.common.Order;
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
public class AnalyzeAuctionData extends LogtoolContext implements Analyzer {
	//	static private Logger log = Logger.getLogger(MktPriceStats.class.getName());

	// service references
	private TimeslotRepo timeslotRepo;
	private TimeService timeService;
	private BrokerRepo brokerRepo;
	private int prevHour = 0;
	private int prevHA = 0;
	// Data
	TreeMap<Integer, Integer> orderbookCounter = new TreeMap<Integer, Integer>();
	private double [] sumClearingPrices = new double [24];
	private double [] countAuction = new double [24];
	private double [] minClearingPriceHA = new double [24];
	private int tempHourAhead = 0;
	private double mintempClearingPrice = 1000.0;
	private int indexOffset = 1; // should be
	// Competition.deactivateTimeslotsAhead - 1
	private PrintWriter output = null;
	private String dataFilename = "clearedTrades.arff";
	private int ignoreInitial = 0; // timeslots to ignore at the beginning
	private int ignoreCount = 0;
	public TreeMap<Integer, TreeMap<Integer, ArrayList<Order>>> producerOrders = new TreeMap<Integer, TreeMap<Integer, ArrayList<Order>>>();
	public TreeMap<Integer, TreeMap<Integer, WeatherForecastPrediction>> weatherPredictions = new TreeMap<Integer,TreeMap<Integer, WeatherForecastPrediction>>();

	/**
	 * Main method just creates an instance and passes command-line args to its
	 * inherited cli() method.
	 */
	public static void main(String[] args) {
		System.out.println("I am running");
		new AnalyzeAuctionData().cli(args);
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
		registerNewObjectListener(new OrderHandler(), Order.class);
		registerNewObjectListener(new TimeslotUpdateHandler(), TimeslotUpdate.class);
		registerNewObjectListener(new ClearedTradeHandler(), ClearedTrade.class);

		ignoreCount = ignoreInitial;
		try {
			//output = new PrintWriter(new File(dataFilename));
			FileWriter fw = new FileWriter(dataFilename, true);
			output = new PrintWriter(new BufferedWriter(fw));
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
		//output.println("hourAhead, AvgClearingPrice, MinClearingPriceCount");
		//		for(int i = 0; i < 24; i++){
		//			output.format("%d, %.2f, %.2f",i,sumClearingPrices[i]/countAuction[i], minClearingPriceHA[i]);
		//			output.println();
		//		}

		/*
		for(Map.Entry<Integer, TreeMap<Integer, ArrayList<Order>>> entry : producerOrders.entrySet()) {
			Integer hour = entry.getKey();
			TreeMap<Integer, ArrayList<Order>> hourvalue = entry.getValue();
			for(Map.Entry<Integer, ArrayList<Order>> subentry : hourvalue.entrySet()) {
				Integer HA = subentry.getKey();
				ArrayList<Order> orders = subentry.getValue();
				WeatherForecastPrediction wfp = weatherPredictions.get(hour).get(HA);
				if(orders != null && wfp != null)
					for(Order o : orders){
						output.println(hour + "," + HA + "," + o.getMWh() + "," + o.getLimitPrice() + "," + 
								wfp.getCloudCover() + "," + wfp.getTemperature() + "," + 
								wfp.getWindDirection() + "," + wfp.getWindSpeed());
					}
				else{
					if(orders == null && wfp == null)
						output.println(hour + "," + HA + ", BOTH_NULL");
					else if(orders == null)
						output.println(hour + "," + HA + ", ORDERS_NULL");
					else{
						output.println(hour + "," + HA + ", WFP_NULL");
					}
				}
			}
		}
		 */
		output.close();
	}


	// -----------------------------------
	// catch TimeslotUpdate events
	class TimeslotUpdateHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			// SimulationDataPerTimeSlot cmt;
			// Working System.out.println("3");
			TimeslotUpdate tsu = (TimeslotUpdate) thing;
			if (ignoreCount-- <= 0) {
				int timeslotSerial = timeslotRepo.currentSerialNumber();
				System.out.println("TimeslotUpdateHandler " + timeslotSerial);
				if(timeslotSerial > 360)
					minClearingPriceHA[tempHourAhead]++;

				tempHourAhead = 0;
				mintempClearingPrice = 1000.0; 
			}
		}
	}

	// -----------------------------------
	// catch ClearedTrade messages
	class ClearedTradeHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			ClearedTrade o = (ClearedTrade) thing;
			int targetTS = o.getTimeslot().getSerialNumber();
			if(targetTS == 384){
				int currentTS = timeslotRepo.getTimeslotIndex(timeService.getCurrentTime());
				output.println("ClearedTrade : " + currentTS + "," + targetTS + ",[" + o.getExecutionMWh() + ";" + o.getExecutionPrice() + "]");
				System.out.println("ClearedTrade : " + currentTS + "," + targetTS + ",[" + o.getExecutionMWh() + ";" + o.getExecutionPrice() + "]");
			}
		}
	}

	
	// -----------------------------------
	// catch ClearedTrade messages
	class OrderHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			Order o = (Order) thing;
			int targetTS = o.getTimeslot().getSerialNumber();
			if(targetTS == 384){
				int currentTS = timeslotRepo.getTimeslotIndex(timeService.getCurrentTime());
				String username = o.getBroker().getUsername();
				output.println("Order : " + username + "," + currentTS + "," + targetTS + ",[" + o.getMWh() + ";" + o.getLimitPrice() + "]");
				System.out.println("Order : " + username + "," + currentTS + "," + targetTS + ",[" + o.getMWh() + ";" + o.getLimitPrice() + "]");
			}
		}
	}
}
