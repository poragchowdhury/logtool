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
import org.powertac.common.msg.CustomerBootstrapData;
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
import org.powertac.common.enumerations.PowerType;
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
public class PerHourDemand extends LogtoolContext implements Analyzer {
	//	static private Logger log = Logger.getLogger(MktPriceStats.class.getName());

	// service references
	private TimeslotRepo timeslotRepo;
	private TimeService timeService;
	private BrokerRepo brokerRepo;
	// Data
	TreeMap<Integer, Integer> orderbookCounter = new TreeMap<Integer, Integer>();
	private double [] sumClearingPrices = new double [24];
	private double [] sumClearingMWh = new double [24];
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
	public ArrayList<String> probrokers = new ArrayList<String>();
	public double netUsage = 0;
	public int counter = 0;
	public double netDemand = 0;
	public int totHours = 0;
	public double sumKWh = 0;
	/**
	 * Main method just creates an instance and passes command-line args to its
	 * inherited cli() method.
	 */
	public static void main(String[] args) {
		System.out.println("I am running");
		new PerHourDemand().cli(args);
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
		probrokers.add("s");
		timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
		timeService = (TimeService) getBean("timeService");
		brokerRepo = (BrokerRepo) SpringApplicationContext
				.getBean("brokerRepo");
		//registerNewObjectListener(new ClearedTradeHandler(), ClearedTrade.class);
		registerNewObjectListener(new TariffTxHandler(), TariffTransaction.class);
		registerNewObjectListener(new BalancingTransactionHandler(), BalancingTransaction.class);
		registerNewObjectListener(new TimeslotUpdateHandler(), TimeslotUpdate.class);
		//registerNewObjectListener(new TimeslotHandler(), Timeslot.class);
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
		output.println("NetDemand " + netDemand + " totHours " + totHours + " perhourdemand " + netDemand/totHours);
		System.out.println("NetDemand " + netDemand + " totHours " + totHours + " perhourdemand " + netDemand/totHours);
		output.close();
	}

	// -----------------------------------
	// catch TariffTransactions
	class TariffTxHandler implements NewObjectListener
	{
		@Override
		public void handleNewObject (Object thing)
		{
			TariffTransaction tx = (TariffTransaction)thing;
			// only include SIGNUP and WITHDRAW
			if(tx.getKWh() <= 0){
				netDemand += tx.getKWh();
				sumKWh += tx.getKWh();
				//System.out.println("TS " + tx.getPostedTimeslotIndex() + " sumKWh " + sumKWh + " totHourCount " + totHours + " KWH " + tx.getKWh());
			}
			else{
				System.out.println(tx.getTxType().toString() + " KWH " + tx.getKWh());
			}
		}
	}

	// -----------------------------------
	// catch TariffTransactions
	class BalancingTransactionHandler implements NewObjectListener
	{
		@Override
		public void handleNewObject (Object thing)
		{
			BalancingTransaction bal = (BalancingTransaction)thing;
			if(bal.getKWh() < 0){
				sumKWh += bal.getKWh();
				netDemand += bal.getKWh();
				System.out.println("BAL : TS " + bal.getPostedTimeslotIndex() + " sumKWh " + sumKWh + " totHourCount " + totHours + " KWH " + bal.getKWh());
			}
		}
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
				totHours = timeslotSerial - 359;
				System.out.println("TimeslotUpdateHandler " + timeslotSerial);
				System.out.println(" sumKWh " + sumKWh + " totHourCount " + totHours);
				output.println("Demand " + sumKWh + " TS " + timeslotSerial + " totHours " + totHours+360);
				sumKWh = 0;
			}
		}
	}
}
