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
import java.util.HashMap;

//import org.apache.log4j.Logger;
import org.joda.time.Instant;
import org.powertac.common.Broker;
import org.powertac.common.BrokerTransaction;
import org.powertac.common.ClearedTrade;
import org.powertac.common.Competition;
import org.powertac.common.MarketTransaction;
import org.powertac.common.Orderbook;
import org.powertac.common.OrderbookOrder;
import org.powertac.common.TimeService;
import org.powertac.common.WeatherForecast;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.repo.OrderbookRepo;
import org.powertac.common.repo.TimeslotRepo;
import org.powertac.common.repo.WeatherReportRepo;
import org.powertac.common.repo.WeatherForecastRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.WeatherReport;
import org.powertac.common.Timeslot;
import org.powertac.common.WeatherForecast;
import org.powertac.common.WeatherForecastPrediction;


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
public class LogExtractor extends LogtoolContext implements Analyzer {
//	static private Logger log = Logger.getLogger(MktPriceStats.class.getName());

	// service references
	private TimeslotRepo timeslotRepo;

	// Data
	private TreeMap<Integer, DataPerTimeSlot> marketData;
	TreeMap<Integer, Integer> orderbookCounter = new TreeMap<Integer, Integer>();
	int counter = 0;
	private int ignoreInitial = 0; // timeslots to ignore at the beginning
	private int ignoreCount = 0;
	private int indexOffset = 0; // should be
									// Competition.deactivateTimeslotsAhead - 1
	public static int numberofbrokers = 0;
	private PrintWriter output = null;
	private PrintWriter debug = null;
	private String dataFilename = "clearedTrades.arff";
	private String writeAttributes = "0";
	public double brokerID;

	//public HashMap<Integer, PredictedClearingPrice> marketClearingPricePredictionV0;

	/**
	 * Main method just creates an instance and passes command-line args to its
	 * inherited cli() method.
	 */
	public static void main(String[] args) {
		System.out.println("I am running");
		System.gc();
		new LogExtractor().cli(args);
	}

	/**
	 * Takes two args, input filename and output filename
	 */
	private void cli(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: <analyzer> input-file output-file");
			return;
		}
		dataFilename = args[1];
		writeAttributes = args[2];
		if(writeAttributes.equalsIgnoreCase("1"))
			System.out.println("its true " + writeAttributes);
		else
			System.out.println("its false " + writeAttributes);
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
		registerNewObjectListener(new CompetitionHandler(), Competition.class);
		registerNewObjectListener(new BrokerHandler(), Broker.class);
		registerNewObjectListener(new TimeslotUpdateHandler(), TimeslotUpdate.class);
		registerNewObjectListener(new TimeslotHandler(), Timeslot.class);
		registerNewObjectListener(new WeatherReportHandler(), WeatherReport.class);
		registerNewObjectListener(new OrderbookHandler(), Orderbook.class);
		registerNewObjectListener(new OrderbookOrderHandler(), OrderbookOrder.class);
		registerNewObjectListener(new WeatherForecastHandler(), WeatherForecast.class);

		ignoreCount = ignoreInitial;

		marketData = new TreeMap<Integer, DataPerTimeSlot>();
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
		
		for (Map.Entry<Integer, DataPerTimeSlot> entry : marketData.entrySet()) {
			Integer timeslot = entry.getKey();
			DataPerTimeSlot trades = entry.getValue();
			
			// Previous hour same hourAhead auction price
			double n_1HourNAuctionPrice[] = new double[24]; 
			double yesterdayNHourNAuctionPrice[] = new double[24];
			double aWeekAgoNHourNAuctionPrice [] = new double[24];
			double aWeeksNHourNAuctionAverageClearingPrice[] = new double[24];
			
			// Same hour's n-1 th auction price : sameHourN_1Price
			double nHourN_1AuctionPrice[] = new double[24];
			double yesterdayNHourN_1AuctionPrice[] = new double[24];
			double aWeekAgoNHourN_1AuctionPrice[] = new double[24];

			
			//**** Previous hour prices *******//
			// Previous hour same hourAhead auction price
			// Same hour's n-1 th auction price : sameHourN_1Price
			// Previous hour same hour's n-1 th auction price avg : previousHourNAvgPrice
			
			if(timeslot > 360){
				for(int hourAheadAuction = 0; hourAheadAuction < 24; hourAheadAuction++){
					int corresponsingTS = timeslot + hourAheadAuction;

					DataPerTimeSlot pHNA1 = marketData.get(corresponsingTS-1);
					if (pHNA1 != null)
						n_1HourNAuctionPrice[hourAheadAuction] = pHNA1.arrClearingPrices[hourAheadAuction];

					
					DataPerTimeSlot pHNA2 = marketData.get(corresponsingTS);
					if (pHNA2 != null)
						nHourN_1AuctionPrice[hourAheadAuction] = pHNA2.arrClearingPrices[hourAheadAuction+1];
				}
			}
			
			//**** Yesterday ago prices *******//
			// 24 prices : yesterdayData [HourAhead]
			// 24 prices : yesterday average price : yesterdayAvg
			// 24 prices : yesterdayN_1Price
			// 24 prices : yesterdayNAvgPrice
			
			if (timeslot-24 > 361)
			{
				for(int hourAheadAuction = 0; hourAheadAuction < 24; hourAheadAuction++){
					int corresponsingTS = (timeslot-24) + hourAheadAuction;

					DataPerTimeSlot pHNA1 = marketData.get(corresponsingTS);
					if (pHNA1 != null)
						yesterdayNHourNAuctionPrice[hourAheadAuction] = pHNA1.arrClearingPrices[hourAheadAuction];

					
					DataPerTimeSlot pHNA2 = marketData.get(corresponsingTS);
					if (pHNA2 != null)
						yesterdayNHourN_1AuctionPrice[hourAheadAuction] = pHNA2.arrClearingPrices[hourAheadAuction+1];
				}
			}
		
			
			//**** A week ago prices *******//
			// 24 prices : aWeekSameHourAverageClearingPrice [HourAhead] 24 prices # Storing a weeks data for same hour auction
			// 24 prices : aweekagoN_1Price # Storing N-1 th clearing price for a week go 
			if ( timeslot-(24*7) > 360){
				for(int hourAheadAuction = 0; hourAheadAuction < 24; hourAheadAuction++){
					int corresponsingTS = (timeslot-(24*7)) + hourAheadAuction;

					DataPerTimeSlot pHNA1 = marketData.get(corresponsingTS);
					if (pHNA1 != null)
						aWeekAgoNHourNAuctionPrice[hourAheadAuction] = pHNA1.arrClearingPrices[hourAheadAuction];

					
					DataPerTimeSlot pHNA2 = marketData.get(corresponsingTS);
					if (pHNA2 != null)
						aWeekAgoNHourN_1AuctionPrice[hourAheadAuction] = pHNA2.arrClearingPrices[hourAheadAuction+1];
				}
				
				
				int count[] = new int[24];
				for(int i = 0; i < 7; i++){
					for(int hourAheadAuction = 0; hourAheadAuction < 24; hourAheadAuction++)
					{
						int corresponsingTS = (timeslot-(24*(i+1))) + hourAheadAuction;

						DataPerTimeSlot pHNA = marketData.get(corresponsingTS); 
						if (pHNA != null){
							if(pHNA.arrClearingPrices[hourAheadAuction] != 0)
								count[hourAheadAuction]++;
							aWeeksNHourNAuctionAverageClearingPrice[hourAheadAuction] += pHNA.arrClearingPrices[hourAheadAuction];
						}
					}
				}
				for(int j = 0; j < 24; j++)
				{
					if(count[j] > 0)
						aWeeksNHourNAuctionAverageClearingPrice[j] /= count[j]; 
				}	
			}
			
			// if (trades.length != 24)
			// log.error("short array " + trades.length);
			// for (int i = 0; i < trades.length; i++) {
			for (int hourAheadAuction = 0; hourAheadAuction < 24; hourAheadAuction++) {
				if (null == trades) {
					//output.print(delim + "[0.0 0.0]");
				} 
				else {
					if(trades.arrClearingPrices[hourAheadAuction] != 0)
					{
						int hourAheadtimeslot = hourAheadAuction + timeslot;
						Timeslot ts = timeslotRepo.findBySerialNumber(hourAheadtimeslot);
					
						//System.out.println("Day_date : "+ ts.getStartTime() + " Month " + ts.getStartTime().getMonthOfYear());
						
						output.format("%d,%d,%d,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f",//%.2f,%.2f,%.2f,%.2f,%.2f,%.2f",
								3,
								ts.getStartTime().getDayOfMonth(), //trades.day_date, 
								ts.getStartTime().getMonthOfYear(), //trades.month_date, 
								ts.dayOfWeek(), //trades.day,
								ts.slotInDay(), //trades.hour, 
								hourAheadAuction,
								//trades.temp, 
								trades.wfTemp[hourAheadAuction+1], 
								//trades.cloudCoverage, 
								trades.wfCloudCover[hourAheadAuction+1], 
								//trades.windDirection, 
								trades.wfWindDir[hourAheadAuction+1], 
								//trades.windSpeed, 
								trades.wfWindSpeed[hourAheadAuction+1], 
								n_1HourNAuctionPrice[hourAheadAuction],
								yesterdayNHourNAuctionPrice[hourAheadAuction],
								aWeekAgoNHourNAuctionPrice[hourAheadAuction], 
								aWeeksNHourNAuctionAverageClearingPrice[hourAheadAuction],  
								nHourN_1AuctionPrice[hourAheadAuction], 
								yesterdayNHourN_1AuctionPrice[hourAheadAuction], 
								aWeekAgoNHourN_1AuctionPrice[hourAheadAuction],
								trades.arrClearingPrices[hourAheadAuction]); 
						output.println();
					}
					
				}	
				
			} 
		}
		output.close(); 
		debug.close();
		System.gc();
	}

	// -----------------------------------
	// catch Broker events
	class BrokerHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			Broker broker = (Broker) thing;
			String username = broker.getUsername().toUpperCase();
			if (username.equals("SPOT_PP3_BS0")) {
				brokerID = broker.getId();
				System.out.println("writing attribute : " + writeAttributes);

				if(writeAttributes.equalsIgnoreCase("1")){
					System.out.println("writing attribute");
					output.println("@relation SPOT");
					output.println("@attribute numberofbrokers real"); 
					output.println("@attribute day_date real"); 
					output.println("@attribute month_date real");
					output.println("@attribute day real"); 
					output.println("@attribute hour real");
					output.println("@attribute hourAhead real");
					output.println("@attribute Temperature real");
					//output.println("@attribute ForecastTemp real");
					output.println("@attribute CloudCover real");
					//output.println("@attribute ForecastCloud real");
					output.println("@attribute WindDirection real");
					//output.println("@attribute ForecastWindD real");
					output.println("@attribute WindSpeed real ");
					//output.println("@attribute ForecastWindS real");
					//output.println("@attribute energyCleared real");
					output.println("@attribute PrevHourClearingPrice real");
					output.println("@attribute YesterdayClearingPrice real");
					output.println("@attribute PrevOneWeekClearingPrice real");
					output.println("@attribute aWeekSameHourAverageClearingPrice real");
					//output.println("@attribute yesterdayAverage real");
					output.println("@attribute PreviousHourN_1Price real");
					output.println("@attribute YesterdayN_1Price real");
					output.println("@attribute AWeekAgoN_1Price real");
					output.println("@attribute ClearingPrice real");
					output.println();
					output.println("@data");
				}
			}
		}
	}
	
	
	class CompetitionHandler implements NewObjectListener{
		@Override
		public void handleNewObject(Object comp){
			System.out.println("Inside Competition handler");
			Competition competition = (Competition) comp;
			MktPriceStatsPorag.numberofbrokers = competition.getBrokers().size();
			System.out.println("Number of brokers : " + competition.getBrokers().size() + " tostring " + competition.toString());
			
		}
	}

	// -----------------------------------
	// catch TimeslotUpdate events
	class TimeslotUpdateHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			// DataPerTimeSlot cmt;
						
			if (ignoreCount-- <= 0) {
				int timeslotSerial = timeslotRepo.currentSerialNumber();
				
				//System.out.println("Inside Timeslot update using timeslot.repo " + timeslotSerial);
					
				DataPerTimeSlot cmt = marketData.get(timeslotSerial);
				int dayOfWeek = timeslotRepo.currentTimeslot().dayOfWeek();
				int dayHour = timeslotRepo.currentTimeslot().slotInDay();
				counter = 0;
				
				System.out.println("TimeslotUpdateHandler: timeslotSerial : " + timeslotSerial);

				if (null == cmt) {
					cmt = new DataPerTimeSlot();
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
				
				int currenttimeslot = timeslotRepo.currentSerialNumber();
				int orderbooktimeslot = ob.getTimeslotIndex();
				int advanceHourAhead = orderbooktimeslot - currenttimeslot;
				DataPerTimeSlot cmt = marketData.get(orderbooktimeslot);
				if (null == cmt) {
					cmt = new DataPerTimeSlot();
				}
				
				System.out.println("OrderbookHandler: Orderbook timeslotSerial : " + orderbooktimeslot + " Current timeslot " + currenttimeslot + " AdvanceHour " + (orderbooktimeslot-currenttimeslot) + " clearing price  " + ob.getClearingPrice());

				if (ob.getClearingPrice() == null){
					OrderbookOrder bid = ob.getBids().last();
					OrderbookOrder ask = ob.getAsks().first();
					cmt.arrClearingPrices[advanceHourAhead] = (Math.abs(bid.getLimitPrice())+Math.abs(ask.getLimitPrice()))/2;
				}
				else {
					cmt.arrClearingPrices[advanceHourAhead] = ob.getClearingPrice();
				}
				
				
				marketData.put(orderbooktimeslot, cmt);
			}
		}
	}

	
	
	// --s---------------------------------
	// catch OrderbookHandler events
	class OrderbookOrderHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			OrderbookOrder ob = (OrderbookOrder) thing;

			if (ignoreCount-- <= 0) {
				
//				int currenttimeslot = timeslotRepo.currentSerialNumber();
//				int orderbooktimeslot = ob.getTimeslotIndex();
//				int advanceHourAhead = orderbooktimeslot - currenttimeslot;
//				DataPerTimeSlot cmt = marketData.get(orderbooktimeslot);
//				if (null == cmt) {
//					cmt = new DataPerTimeSlot();
//				}
//				
				//System.out.println("OrderbookHandler: Orderbook timeslotSerial : " + orderbooktimeslot + " Current timeslot " + currenttimeslot + " AdvanceHour " + (orderbooktimeslot-currenttimeslot) + " clearing price  " + ob.getClearingPrice());

//				if (ob.getClearingPrice() == null){
//					OrderbookOrder bid = ob.getBids().last();
//					OrderbookOrder ask = ob.getAsks().first();
//					cmt.arrClearingPrices[advanceHourAhead] = (Math.abs(bid.getLimitPrice())+Math.abs(ask.getLimitPrice()))/2;
//				}
//				else {
//					cmt.arrClearingPrices[advanceHourAhead] = ob.getClearingPrice();
//				}
				
				
				//marketData.put(orderbooktimeslot, cmt);
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
				
				DataPerTimeSlot cmt = marketData.get(timeslotSerial);
				if (null == cmt) {
					cmt = new DataPerTimeSlot();
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
				System.out.println("WeatherReportHandler : Currenttimeslot :" + currenttimeslot + " Weather Report for : " + timeslotSerial);
				//debug.println("Currenttimeslot :" + currenttimeslot + " Weather Report for : " + timeslotSerial);

			}
		}
	}
	
	class WeatherForecastHandler implements NewObjectListener {
		
		@Override
		public void handleNewObject(Object thing) {
			
			WeatherForecast wf = (WeatherForecast) thing;
			//System.out.println("In the weather forecast Prediction handler");
			
			if (ignoreCount-- <= 0) {
				int advanceHour;
				int currenttimeslot = timeslotRepo.currentSerialNumber();
				int weathertimeslot = wf.getTimeslotIndex();
			
				System.out.println("WeatherForecastHandler: Currenttimeslot serial : " + currenttimeslot + " weatherForecasttimeslotserial : " + weathertimeslot);
				
				DataPerTimeSlot cmt = marketData.get(weathertimeslot);
				if (null == cmt) {
					cmt = new DataPerTimeSlot();
				}
				
				for(int i = 0; i < wf.getPredictions().size(); i++)
				{
					WeatherForecastPrediction wfp = wf.getPredictions().get(i);
					advanceHour = wfp.getForecastTime();
					//System.out.println("AdvanceHour" + advanceHour + " Temp " + wfp.getTemperature());
					if(advanceHour > 0)
					{
						
						cmt.wfTemp[advanceHour] = wfp.getTemperature();
						cmt.wfCloudCover[advanceHour] = wfp.getCloudCover();
						cmt.wfWindDir[advanceHour] = wfp.getWindDirection();
						cmt.wfWindSpeed[advanceHour] = wfp.getWindSpeed();
					//	System.out.println("CurrentTimeslot " + currenttimeslot + " Weather Forecast for : " + timeslotSerial +" hourAhead " + advanceHour);
						debug.println("CurrentTimeslot " + currenttimeslot + " Weather Forecast for : " + weathertimeslot +" hourAhead " + advanceHour);
					}
				}
 				marketData.put(weathertimeslot, cmt);
 				//System.out.println("OK");
  				
			}
		}
	}


	class TimeslotHandler implements NewObjectListener {
		@Override
		public void handleNewObject(Object thing) {
			if (ignoreCount > 0) {
				return;
			}
			Timeslot ts = (Timeslot) thing;
			int target = ts.getSerialNumber();
			System.out.println("TimeslotHandler : Inside timeslot handler : ts.getPostedTimeslot : " + target);

			DataPerTimeSlot cmt = marketData.get(target);
			if (null == cmt) {
				cmt = new DataPerTimeSlot();

			}
			marketData.put(target, cmt);
		}
	}

}

class DataPerTimeSlot {
	int month_date;
	int day_date;
	int timeslotIndex;
	double boughtMWh;
	double soldMWh;
	double mWh;
	double price;
	double boughtprice;
	double soldprice;
	int count;
	double balancingTrans;
	int day;
	int hour;
	double temp;
	double cloudCoverage;
	double windSpeed;
	double windDirection;
	double arrClearingPrices[] = new double[25];
	double wfTemp[] = new double[25];
	double wfCloudCover[] = new double[25];
	double wfWindDir[] = new double[25];
	double wfWindSpeed[] = new double[25];
	double energyCleared;
	
	DataPerTimeSlot() {
		month_date = 0;
		day_date = 0;
		timeslotIndex = 0;
		boughtMWh = 0.0;
		soldMWh = 0.0;
		boughtprice = 0.0;
		soldprice = 0.0;
		mWh = 0.0;
		price = 0.0;
		count = 0;
		balancingTrans = 0.0;
		day = 0;
		hour = 0;
		temp = 0.0;
		cloudCoverage = 0.0;
		windSpeed = 0.0;
		windDirection = 0.0;
		energyCleared = 0.0;
	}
}
