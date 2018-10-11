/*
 * Copyright (c) 2015 by the original author
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
import java.util.Map.Entry;
import java.util.TreeMap;

//import org.apache.log4j.Logger;
import org.powertac.common.BalancingTransaction;
import org.powertac.common.Broker;
import org.powertac.common.Competition;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;
import org.powertac.util.Pair;

/**
 * Example analysis class.
 * Extracts TariffTransactions, looks for SIGNUP and WITHDRAW transactions,
 * computes market share by broker and total customer count.
 * 
 * First line lists brokers. Remaining lines are emitted for each timeslot
 * in which SIGNUP or WITHDRAW transactions occur, format is
 *   timeslot, customer-count, ..., total-customer-count
 * with one customer-count field for each broker.
 * 
 * @author John Collins
 */
public class TariffPriceStats
extends LogtoolContext
implements Analyzer
{
//	static private Logger log = Logger.getLogger(TariffPriceStats.class.getName());
	private final String separator = ",";

	private DomainObjectReader dor;

	private BrokerRepo brokerRepo;

	// list of TariffTransactions for current timeslot
	private ArrayList<TariffTransaction> ttx;
	private HashMap<Broker, Integer> customerCounts;

	// output array, indexed by timeslot
	private ArrayList<Broker> brokers = null;
	private HashMap<Integer,HashMap<Broker, Pair<Double, Double>>> usage = null;

	// data output file
	private PrintWriter data = null;
	private String dataFilename = "data.txt";
	
	private int currentTimeslot = 0;

	/**
	 * Constructor does nothing. Call setup() before reading a file to
	 * get this to work.
	 */
	public TariffPriceStats ()
	{
		super();
	}
	
	/**
	 * Main method just creates an instance and passes command-line args to
	 * its inherited cli() method.
	 */
	public static void main (String[] args)
	{
		new TariffPriceStats().cli(args);
	}
	
	/**
	 * Takes two args, input filename and output filename
	 */
	private void cli (String[] args)
	{
		if (args.length != 2) {
			System.out.println("Usage: <analyzer> input-file output-file");
			return;
		}
		dataFilename = args[1];
		super.cli(args[0], this);
	}

	/**
	 * Creates data structures, opens output file. It would be nice to dump
	 * the broker names at this point, but they are not known until we hit the
	 * first timeslotUpdate while reading the file.
	 */
	@Override
	public void setup ()
	{
		//dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
		brokerRepo = (BrokerRepo) SpringApplicationContext.getBean("brokerRepo");
		ttx = new ArrayList<TariffTransaction>();
		usage = new HashMap<Integer,HashMap<Broker, Pair<Double, Double>>>(2000);

		registerNewObjectListener(new TimeslotUpdateHandler(),TimeslotUpdate.class);
		registerNewObjectListener(new TariffTxHandler(),TariffTransaction.class);
		try
		{
			data = new PrintWriter(new File(dataFilename));
		}
		catch (FileNotFoundException e)
		{
//			log.error("Cannot open file " + dataFilename);
		}
		data.println("timeslot" + separator + "broker" + separator + "totalKWh" + separator + "totalCharge");
	}

	@Override
	public void report ()
	{
		for (Entry<Integer, HashMap<Broker, Pair<Double, Double>>> timeslot : usage.entrySet())
		{
			int time = timeslot.getKey(); 
			HashMap<Broker, Pair<Double, Double>> thisTimeslot = timeslot.getValue();
			for (Entry<Broker, Pair<Double, Double>> brokerSet : thisTimeslot.entrySet())
			{
				data.println(time + separator +
						brokerSet.getKey().getUsername() + separator +
						brokerSet.getValue().car() + separator +
						brokerSet.getValue().cdr());
			}
		}
		data.close();
	}

	// Called on timeslotUpdate. Note that there are two of these before
	// the first "real" timeslot. Incoming tariffs are published at the end of
	// the second timeslot (the third call to this method), and so customer
	// consumption against non-default broker tariffs first occurs after
	// four calls.
	private void summarizeTimeslot (TimeslotUpdate ts)
	{
		currentTimeslot = ts.getFirstEnabled() - 2;

		if (null == brokers)
		{
			// first time through
			brokers = new ArrayList<Broker>();
			for (Broker broker : brokerRepo.findRetailBrokers())
			{
				brokers.add(broker);
			}
		}

		if (ttx.size() > 0)
		{
			// there are some signups and withdraws here
			for (TariffTransaction tx : ttx)
			{
				Broker broker = tx.getBroker();
				double thisKWh;
				double thisCharge;
				if (!usage.containsKey(currentTimeslot))
					usage.put(currentTimeslot, new HashMap<Broker, Pair<Double, Double>>());
				if (!usage.get(currentTimeslot).containsKey(broker))
				{
					HashMap<Broker, Pair<Double, Double>> thisTimeslot = usage.get(currentTimeslot);
					thisTimeslot.put(broker, new Pair<Double, Double>(0.0, 0.0));
				}
				HashMap<Broker, Pair<Double, Double>> thisUsage = usage.get(currentTimeslot);
				thisKWh = thisUsage.get(broker).car();
				thisCharge = thisUsage.get(broker).cdr();
				thisKWh += tx.getKWh();
				thisCharge += tx.getCharge();
				thisUsage.put(broker, new Pair<Double, Double>(thisKWh, thisCharge));
				usage.put(currentTimeslot, thisUsage);
			}
		}
		ttx.clear();
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
			if (tx.getTxType() == TariffTransaction.Type.CONSUME || tx.getTxType() == TariffTransaction.Type.PUBLISH)
			{
				ttx.add(tx);
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
			summarizeTimeslot((TimeslotUpdate)thing);
		}
	}
}