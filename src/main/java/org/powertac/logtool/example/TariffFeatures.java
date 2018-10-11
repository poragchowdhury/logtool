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
import java.util.List;

import org.powertac.common.Broker;
import org.powertac.common.Rate;
import org.powertac.common.TariffSpecification;
import org.powertac.common.TariffTransaction;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

/**
 * Examines the features of the tariffs published in a game.
 * Extracts TariffTransactions, looks for PUBLISH transactions, then extracts the information for future analysis. Data
 * is outputted in Markdown format for easy reading.
 *
 * @author Russell Folk
 */
public class TariffFeatures
        extends LogtoolContext
        implements Analyzer
{
    //static private Logger log = Logger.getLogger(TariffFeatures.class.getName());

    private BrokerRepo brokerRepo;

    // list of TariffTransactions for current timeslot
    private ArrayList<TariffTransaction> ttx;

    // output array, indexed by timeslot
    private ArrayList<Broker> brokers = null;

    // broker Tariffs Published
    private HashMap<Broker, ArrayList<TariffSpecification>> publishedTariffs;

    // data output file
    private PrintWriter data = null;
    private String dataFilename = "data.txt";

    /**
     * Constructor does nothing. Call setup() before reading a file to
     * get this to work.
     */
    private TariffFeatures()
    {
        super();
    }

    /**
     * Main method just creates an instance and passes command-line args to
     * its inherited cli() method.
     */
    public static void main (String[] args)
    {
        new TariffFeatures().cli(args);
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
        DomainObjectReader dor;
        //dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
        brokerRepo = (BrokerRepo) SpringApplicationContext.getBean("brokerRepo");
        ttx = new ArrayList<>();

        registerNewObjectListener(new TimeslotUpdateHandler(),
                TimeslotUpdate.class);
        registerNewObjectListener(new TariffTxHandler(),
                TariffTransaction.class);
        try {
            data = new PrintWriter(new File(dataFilename));
        }
        catch (FileNotFoundException e) {
            //log.error("Cannot open file " + dataFilename);
        }
    }

    @Override
    public void report ()
    {
        data.println("# Broker Tariff Features");
        data.println();

        for (Broker broker : brokers)
        {
            data.println("## " + broker + "'s Tariffs");
            ArrayList<TariffSpecification> tariffs = publishedTariffs.get(broker);
            for (int i = 0; i < tariffs.size(); i++)
            {
                printSpecification(tariffs.get(i), (i+1));
            }
        }

        data.close();
    }

    // Called on timeslotUpdate. Note that there are two of these before
    // the first "real" timeslot. Incoming tariffs are published at the end of
    // the second timeslot (the third call to this method), and so customer
    // consumption against non-default broker tariffs first occurs after
    // four calls.
    private void summarizeTimeslot ()
    {
        if (null == brokers) {
            // first time through
            brokers = new ArrayList<>();
            publishedTariffs = new HashMap<>();
            for (Broker broker : brokerRepo.findRetailBrokers()) {
                brokers.add(broker);
                publishedTariffs.put(broker, new ArrayList<>());
            }
        }

        if (ttx.size() > 0) {
            // there are some signups and withdraws here
            for (TariffTransaction tx : ttx) {
                Broker broker = tx.getBroker();
                ArrayList<TariffSpecification> currentBroker = publishedTariffs.get(broker);
                currentBroker.add(tx.getTariffSpec());
                publishedTariffs.put(broker, currentBroker);
            }
        }
        ttx.clear();
    }

    private void printSpecification(TariffSpecification spec, int number)
    {
        data.println(number + ".\tTariff Type: " + spec.getPowerType());
        data.println();
        data.print("\tSignup Bonus: ");
        if (spec.getSignupPayment() != 0.0)
            data.println("" + spec.getSignupPayment());
        else
            data.println("No");
        data.println();
        data.print("\tEarly withdrawal fee: ");
        if (spec.getEarlyWithdrawPayment() != 0.0)
        {
            data.print("" + spec.getEarlyWithdrawPayment());
            data.println();
            data.println("Minimum time: " + spec.getMinDuration());
        }
        else
            data.println("No");
        data.println();
        data.print("\tPeriodic Payment: ");
        if (spec.getPeriodicPayment() != 0.0)
            data.println("" + (spec.getPeriodicPayment()));
        else
            data.println("No");
        data.println();

        List<Rate> rates = spec.getRates();

        for (int rateNum = 0; rateNum < rates.size(); ++rateNum)
        {
            data.print("\t" + number + "." + (rateNum+1) +"\t");
            Rate rate = rates.get(rateNum);
            if (rate.isTimeOfUse())
            {
                data.println("Time Variable Rate");
                data.println();
                data.print("\t\tBegins: ");
                if (rate.getWeeklyBegin() > -1 || rate.getWeeklyEnd() > -1)
                    data.print(rate.getWeeklyBegin() + " @ ");
                data.println(rate.getDailyBegin() + ":00");
                data.println();
                data.print("\t\tEnds: ");
                if (rate.getWeeklyBegin() > -1 || rate.getWeeklyEnd() > -1)
                    data.print(rate.getWeeklyEnd() + " @ ");
                data.println(rate.getDailyEnd() + ":00");
            }
            else
                data.println("All-Day / Every-Day Rate");
            data.println();

            if (!rate.isFixed())
            {
                data.println("\t\tVariable Cost");
                data.println();
                data.println("\t\tMinimum: €" + (rate.getMinValue()));
                data.println();
                data.println("\t\tMaximum: €" + (rate.getMaxValue()));
                data.println();
                data.println("\t\tExpected: €" + (rate.getExpectedMean()));
            }
            else
                data.println("\t\tCost: €" + (rate.getValue()));
            data.println();
            if (rate.getTierThreshold() != 0.0)
            {
                data.println("\t\tTiered Threshold: " + rate.getTierThreshold());
                data.println();
            }
            if (rate.getMaxCurtailment() != 0.0)
            {
                data.println("\t\tMax Curtailment: " + rate.getMaxCurtailment());
            }
        }
        data.println();
    }

    // -----------------------------------
    // catch TariffTransactions
    class TariffTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject (Object thing)
        {
            TariffTransaction tx = (TariffTransaction)thing;
            // only include new tariffs
            if (tx.getTxType() == TariffTransaction.Type.PUBLISH) {
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
            summarizeTimeslot();
        }
    }
}
