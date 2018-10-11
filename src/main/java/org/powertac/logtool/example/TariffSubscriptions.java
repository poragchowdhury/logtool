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

//import org.apache.commons.lang.ObjectUtils;
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
public class TariffSubscriptions
        extends LogtoolContext
        implements Analyzer
{
    //static private Logger log = Logger.getLogger(TariffSubscriptions.class.getName());

    private DomainObjectReader dor;

    private BrokerRepo brokerRepo;

    // list of TariffTransactions for current timeslot
    private ArrayList<TariffTransaction> ttx;
    //private HashMap<Broker, Integer> customerCounts;
    private HashMap<Broker, HashMap<Long, Integer>> brokerTariffCounts;

    // output array, indexed by timeslot
    private ArrayList<Broker> brokers = null;

    // data output file
    private PrintWriter data = null;
    private String dataFilename = "data.txt";

    /**
     * Constructor does nothing. Call setup() before reading a file to
     * get this to work.
     */
    public TariffSubscriptions ()
    {
        super();
    }

    /**
     * Main method just creates an instance and passes command-line args to
     * its inherited cli() method.
     */
    public static void main (String[] args)
    {
        new TariffSubscriptions().cli(args);
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

        registerNewObjectListener(new TimeslotUpdateHandler(),
                TimeslotUpdate.class);
        registerNewObjectListener(new TariffTxHandler(),
                TariffTransaction.class);
        try {
            data = new PrintWriter(new File(dataFilename));
        }
        catch (FileNotFoundException e) {
           // log.error("Cannot open file " + dataFilename);
        }
    }

    @Override
    public void report ()
    {
        data.close();
    }

    // Called on timeslotUpdate. Note that there are two of these before
    // the first "real" timeslot. Incoming tariffs are published at the end of
    // the second timeslot (the third call to this method), and so customer
    // consumption against non-default broker tariffs first occurs after
    // four calls.
    private void summarizeTimeslot (TimeslotUpdate ts)
    {
        int currentTimeslot = ts.getFirstEnabled() - 2;

        if (null == brokers) {
            // first time through
            brokers = new ArrayList<Broker>();
            //customerCounts = new HashMap<Broker, Integer>();
            brokerTariffCounts = new HashMap<Broker, HashMap<Long, Integer>>();

            data.print("ts, ");
            data.print("broker, ");
            data.print("tariff id, ");
            data.print("power type, ");
            data.println("customers");
            for (Broker broker : brokerRepo.findRetailBrokers()) {
                brokers.add(broker);
                //customerCounts.put(broker, 0);
                brokerTariffCounts.put(broker, new HashMap<Long, Integer>());
                //data.print(broker.getUsername());
                //data.print(", ");
            }
            //data.println("total");
        }

        if (ttx.size() > 0) {
            // there are some signups and withdraws here
            data.print(currentTimeslot + ", ");
            for (TariffTransaction tx : ttx) {
                Broker broker = tx.getBroker();
                data.print(broker.getUsername() + ", ");
                long tariffID = tx.getId();
                data.print(tariffID + ", ");
                data.print(tx.getTariffSpec().getPowerType());
                int pop = 0;
                if (tx.getTxType() == TariffTransaction.Type.SIGNUP)
                    pop = tx.getCustomerCount();
                else if (tx.getTxType() == TariffTransaction.Type.WITHDRAW)
                    pop = -tx.getCustomerCount();
                HashMap<Long, Integer> brokerMap;
                if (brokerTariffCounts.get(broker).get(tariffID) == null)
                {
                    brokerMap = new HashMap<Long, Integer>();
                }
                else
                {
                    brokerMap = brokerTariffCounts.get(broker);
                }
				try{
					brokerMap.put(tariffID, pop + brokerMap.get(tariffID));
                }
				catch(Exception ex){
					System.out.println("Nullpointer exception : Line 188");
				}
				brokerTariffCounts.put(broker, brokerMap);
                data.println(brokerMap.get(tariffID));
                //customerCounts.put(broker, customerCounts.get(broker) + pop);
            }
            // print results for this timeslot

            /*int sum = 0;
            for (Broker broker: brokers) {
                int count = customerCounts.get(broker);
                data.print(count);
                sum += count;
                data.print(", ");
            }
            data.println(sum);*/
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
            if (tx.getTxType() == TariffTransaction.Type.SIGNUP ||
                    tx.getTxType() == TariffTransaction.Type.WITHDRAW) {
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