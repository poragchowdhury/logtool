package org.powertac.logtool.example;

import java.util.Arrays;

class SimulationDataPerTimeSlot {
	int month_date;
	int day_date;
	int timeslotIndex;
	double capacityTransaction;
	
	double energyBought;
	double energySold;
	double netEnergy;
	double netPrice;
	double marketCost;
	double marketGain;
	
	double [] arrenergyBought = new double[12];
	double [] arrenergySold= new double[12];
	double [] arrnetEnergy= new double[12];
	double [] arrnetPrice= new double[12];
	double [] arrmarketCost= new double[12];
	double [] arrmarketGain= new double[12];
	double [] arrcashPosition = new double[12];
	
	double boughtprice;
	double soldprice;
	double marketOverallBalance;
	int count;
	double soldPriceB [] = new double[12];
	double boughtPriceB [] = new double[12];
	double netPriceB [] = new double[12];
	double balEngDefct [] = new double[12];
	double balEngSurpls [] = new double[12];
	double balancingKWH [] = new double[12]; 
	
	double netDistributionFee;
	double tariffGain;
	double tariffNetPrice;
	double cashPosition;
	double bankNet;
	double rate;
	int day;
	int hour;
	double temp;
	double cloudCoverage;
	double windSpeed;
	double windDirection;
	double lastclearingPrice;
	double arrClearingPrices[] = new double[25];
	double wfTemp[] = new double[25];
	double wfCloudCover[] = new double[25];
	double wfWindDir[] = new double[25];
	double wfWindSpeed[] = new double[25];
	double energyCleared;
	double distributionCost;
	double bankCost;
	double bankGain;
	double tariffCost;
	double tariffGains;
	double market[] = new double[12];
	double tariff[] = new double[12];
	double tariffUsage[] = new double[12];
	double distribution[] = new double[12];
	double balancing[] = new double[12];
	double bank[] = new double[12];
	double arrMarketBuy[] = new double[12];
	double arrMarketSell[] = new double[12];
	double arrMarketBuyMWh[] = new double[12];
	double arrMarketSellMWh[] = new double[12];
	double arrTradeCount[] = new double[12];
	double arrTradeCountBuy[] = new double[12];
	double arrTradeCountSell[] = new double[12];
	double arrCapacityTransaction[] = new double[12];
	
	SimulationDataPerTimeSlot() {
		month_date = 0;
		day_date = 0;
		timeslotIndex = 0;
		energyBought = 0.0;
		energySold = 0.0;
		boughtprice = 0.0;
		soldprice = 0.0;
		netEnergy = 0.0;
		netPrice = 0.0;
		marketOverallBalance = 0.0;
		count = 0;
		netDistributionFee = 0.0;
		tariffNetPrice = 0.0;
		cashPosition = 0.0;
		bankNet = 0.0;
		rate = 0.0;
		day = 0;
		hour = 0;
		temp = 0.0;
		cloudCoverage = 0.0;
		windSpeed = 0.0;
		windDirection = 0.0;
		lastclearingPrice = 0.0;
		energyCleared = 0.0;
		marketCost = 0.0;
		marketGain = 0.0;
		distributionCost = 0.0;
		bankCost = 0.0;
		bankGain = 0.0;
		tariffCost = 0.0;
		tariffGain = 0.0;
		capacityTransaction = 0;
	}
}
