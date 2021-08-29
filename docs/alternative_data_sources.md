# Alternative data sources

Interactive Brokers can be a great source of contract price data but when your portfolio grows, you might start looking for alternative data sources. This is mainly because downloading price data from IB can be really time consuming due to the delays required to avoid pacing violation.  Also in contrast to the Desktop TWS in which delayed market data is available on any contracts, when using the API with IB gateway you are required to have market data subscriptions. This can be quite costly if you want to access wide range of markets and especially should you be classified as Professional client which could happen for various reasons, such as working for a bank.

There are now two new modules for pysystemtrade which allows us to use futures price data from alternative data sources: [Norgate](https://norgatedata.com/) and [Commodity Systems Inc (CSI) Data](http://www.csidata.com/).  

Norgate has around 100 futures contracts available and includes most of the futures that you might want to trade. What they don't offer however is some of the smaller mini and micro futures. CSI on the other hand has around 1400 futures in their database and as such is a cornucopia for data mining if you want to do for example correlation analysis on a wider range of markets. Both are reasonable priced, around 20-50 USD/month depending on the subscription type.  

Unfortunately both sources require Windows clients that function as an intermediary between their own data servers and pysystemtrade, so if you are running pysystemtrade on a Linux machine you then need to have another machine (or perhaps a virtual machine, such as VMWare or Proxmox) running Windows. 

Both clients can be configured to write available market data into CSV files, which can then be accessed for example on a shared network file folder or they can be automatically copied to your pysystemtrade machine. **csvFuturesContractPriceData** class works as a data broker interface which instantiates a **parametric CSV database**  which then reads the folder structure, creates a catalog of available contracts and does any additional processing in order to convert the data in CSV to the format pysystemtrade understands.  Automatic daily updates from each data source can be configured by adding a **historical_data_sources** entry into your *private_config.yaml*. 

See below for more detailed installation instructions.

## Norgate Data

After buying the subscription you need to download and install the **Norgate Data Updater (NDU)** Windows client. 

With Norgate you have actually two ways to access the price data. The NDU client offers its own API which can be used with a [Python module](https://pypi.org/project/norgatedata/) for which *sysdata.norgate.norgateFuturesContractPriceData* acts as a wrapper. Unfortunately the client doesn't allow remote access from another machine so pysystemtrade needs to be running on the same Windows (virtual) machine for the API access to work. This restriction can be circumvented though with some technical expertise, but since it could be a violation of their license agreement the details are not included here. 

Another way to access the contract price data is to use *Export Task Manager* (from Tools menu) so that CSV files will be created and subsequently updated every time new prices are downloaded from the server. This process can be automated by enabling  'Run Export Tasks after: Manual and Scheduled update'  from Settings->Update Mode.

Create a new **Export Task** with following settings:
  - Select "Futures"
  - Date Padding: No Padding
  - Field Selection: Open, High, Low, Close, Volume
	  - You could also add 'Open Interest' but it's not used by pysystemtrade yet
  - Date Format: yyyMMdd
  - Field delimiter: COMMA
  - Decimal Separator: PERIOD
  - Wrap field in Quotes: No
  - Insert Header Row: Yes
  - Periodicity: Daily
  - Export all
  - Destination folder: your shared network folder

Annoyingly the client will spit all the CSV files into one single folder where you will now have circa 25000 files laying around..

### Seed initial data from Norgate

You can use the scripts *sysinit/futures/seed_price_data_from_norgate.py* (Python API) or *sysinit/futures/seed_price_data_from_norgate_csv.py* (CSV database) to initialize contract price data for either single instruments or all instruments available from Norgate. 

After this you should of course create roll calendar for each instrument and then create multiple and adjusted price series as instructed [here](https://github.com/robcarver17/pysystemtrade/blob/master/docs/data.md#roll-calendars)

### Automating daily data updates via Norgate Python API

For updating prices manually you can use the script *sysproduction/update_historical_prices_from_norgate.py* but for automating the process you can insert following section into your *private_config.yaml*:
```
historical_data_sources:
  IB:
    func: "sysproduction.update_historical_prices_from_ib.update_historical_prices"
    config:
      # all available instruments are included by default
      include_instruments: ALL
      # Then we exclude all instruments that are available from Norgate
      exclude_instruments: ['AUD', 'BOBL', 'BTP', 'BUND', 'BUXL', 'CAC', 
          'CAD', 'CHF', 'COCOA', 'COFFEE', 'COPPER', 'CORN', 'COTTON2', 'CRUDE_W', 
          'DAX', 'DX', 'EDOLLAR', 'ETHANOL', 'EUA', 'EUR', 'EUROSTX', 'FED', 'FEEDCOW', 
          'FTSE100', 'FTSECHINAA', 'GASOILINE', 'GAS_US', 'GBP', 'GICS', 'GOLD', 
          'HEATOIL', 'JGB-SGX-mini', 'JPY', 'KOSPI', 'LEANHOG', 'LIVECOW', 'LUMBER', 
          'MILK', 'MSCISING', 'MXP', 'NIFTY', 'NIKKEI-JPY', 'NZD', 'OATIES', 'OJ', 
          'PALLAD', 'PLAT', 'REDWHEAT', 'RICE', 'ROBUSTA', 'SHATZ', 'SILVER', 'SMI',
          'SOYBEAN', 'SOYMEAL', 'SOYOIL', 'SP400', 'SP500', 'SPI200', 'STERLING3', 
          'SUGAR11', 'US10', 'US10U', 'US2', 'US20', 'US30', 'US5', 'VIX', 'WHEAT']
  Norgate:
    func: "sysproduction.update_historical_prices_from_norgate.update_historical_prices"
```
This will instruct IB to download daily and intraday for all instruments that Norgate doesn't cover. As Norgate supplies only daily data the intraday prices would be missing. If you still would like to have intraday data for these instruments, we can configure pysystemtrade as below:

```
historical_data_sources:
  IB_daily: # downloads daily data for non-Norgate instruments
    func: "sysproduction.update_historical_prices_from_ib.update_historical_prices"
    config:
      # All available instruments are included by default
      # Then we exclude all instruments that are available from Norgate
      exclude_instruments: ['AUD', 'BOBL', 'BTP', 'BUND', 'BUXL', 'CAC']
      # .. and many other instruments. For full list see the first Norgate example above

      frequency: 'D'  # daily data only
  IB_intraday:  # downloads hourly data for all instruments
    func: "sysproduction.update_historical_prices_from_ib.update_historical_prices"
    config:
      # All available instruments are included by default
      frequency: 'H' 
  Norgate:
    func: "sysproduction.update_historical_prices_from_norgate.update_historical_prices"
```
For more detailed info about the **historical_data_sources** configuration, see section below.

### Automating daily data updates from Norgate CSV files

For Norgate CSV files we don't have a distinct processing class but instead the files are processed by universal CSV reader (Parametric CSV database).

For updating prices manually you can use the script *sysproduction/update_historical_prices_from_norgate_csv.py* but for automating the process you can insert following section into your *private_config.yaml*:
```
historical_data_sources:
  IB:
    func: "sysproduction.update_historical_prices_from_ib.update_historical_prices"
    config:
      # All available instruments are included by default
      # Then we exclude all instruments that are available from Norgate
      exclude_instruments: ['AUD', 'BOBL', 'BTP', 'BUND', 'BUXL', 'CAC'] 
      # .. and many other instruments. For full list see the first Norgate example above

  NorgateCSV:
    func: "sysproduction.update_historical_prices_from_csv.update_historical_prices"
    config:
      csv_config_factory_func: "sysdata.norgate.ng_database.csv_config_factory"
      csv_datapath: "/your/shared/network/folder/for/Norgate/futures"
      csv_config:
        ignore_missing_symbols: ALL   # prevent spamming log because we have so many unmapped broker symbols
```
For more information about CSV processing and Parametric CSV database, see section 'Custom CSV database' below

## CSI Data

After subscribing to their market data service you can install and unlock the **Unfair Advantage** program which you can use to automate the downloading of price data from their server. With UA you have to create a portfolio (a collection of futures contracts that you are interested in) to have actual access to the market data. 

### Screening data for portfolio 

Even though their database has circa 1400 futures contracts, you can have active portfolios accessing only 150 of them at the time so the first thing you would want to do is screen the database for those contracts that are available for trading on IB and are otherwise suitable (such as having enough liquidity). 

**TLDR:** There's an example portfolio (*sysdata/csi/example_portfolio.csv*) of 150 instruments ready for you if you want to get straight to the business.

In order to screen the suitable futures we have a script just for that purpose: *sysdata/csi/csi_portfolio_constructor.py*.  You can modify it if you want to fiddle with the minimum volume criteria or force adding or excluding certain instruments. The script will do additional checks (such as cross-checking the CSI instrument database against that of pysystemtrade) to make sure the portfolio will be valid and consistent. We wouldn't like to erroneously download pricing data for instance for Wheat (USD on CBOT) when we are actually trading Milling wheat (EUR on EURONEXT). This cross-checking is especially important should you map new instruments from CSI database to pysystemtrade.

Before you can use the screening or validating scripts, you have to fetch CSI's full version of market metadata database. For legal reasons it's not included in Github, but you can save it yourself in UA:
 - From main menu access 'Market specs'
 -  Check 'Include Latest Pricing Information' 
 -  File menu -> Save table results to file
 - Save it as **'markets.csv'** to *sysdata/csi/*

This file contains now all the required metadata for the circa 1400 futures instruments available in UA.

### Importing portfolio file

Create a new portfolio on Unfair Advantage by importing the portfolio CSV file created earlier:
 - Portfolio -> Import Custom Portfolio -> ASCII import
 - Field 1: CSI #
 - Field 2: Ignore
 - Include "Commodities"
 - Field Delimiter: Comma
 - Target portfolio: New Portfolio
 - Import

Edit portfolio window opens:
 - Give the portfolio a suitable name such as "CSI150"
 - Folder: Your designated shared network folder
  - "ASCII/Excel files" tab  
       - Name By: "#_S_CY_A" without quotes
       - Use generic filename override for continuous series: CHECKED (if you want to fiddle with precalculated adjusted/continuous contracts)
       - Export Futures into Symbolized Directories: CHECKED  (so you don't have tens of thousands contracts in same directory.. :)
   - "ASCII/Excel fields" tab
       - Fields: "DOHLCv" without quotes. (there are also many other interesting fields such as open interest, expiration date, spot pricing etc.)
       - Separator: comma
       - Date separator: none
       - Date Format: YYYY/MM/DD
   - "CSI Format" tab:
	   - Files per directory: 999
   - "ASCII/Excel" tab
	   - Include a Header record that provides column definitions: CHECKED
   - Other settings should be fine as default. 
 
Set Portfolio Details window opens:
- File format: ASCII
- Start date & Stop date: As available
- Periodicity: D

When asked about building exported databases, skip this step for now. 

All the instruments are imported by default as Back-Adjusted contracts but we need the separate monthly contracts instead so we must change this.  In Portfolio manager view select all instruments (select first one, scroll down to the last, hold shift and click the last instrument), then click 'Edit Symbols'. Change selection from 'Back-Adjust' to 'Futures'.  Press 'X' next to 'Delivery' to select all possible monthly contracts and then press OK. After a few CPU cycles you have now circa 28000 monthly contracts in your portfolio.  Now you can build the exported database, and approximately 2.7 cups of coffee later you will have the CSV files in your shared network folder, each instrument separated neatly in its own subdirectory.

### Automating database updates

Annoyingly the UA software doesn't fetch price updates automatically and the 'AutoSchedule' setting under 'Download Preferences' doesn't seem to work at least for me. Alternative way to automate this is to create a new task in Windows' **Task Scheduler** to execute UA's  **EZDownloader.exe** on a suitable schedule (for example Tuesday-Saturday 2.30 AM UTC+0).  The online help of UA has more detailed steps how to do this.

**IMPORTANT:** The Unfair Advantage program itself MUST NOT be running when EZDownloader is executed. Otherwise the update process will just silently fail.  Please check the CSV file timestamps or UA's Update Report Logs after you've setup your system to make sure that updates are actually happening.

### Seed initial data from CSI

You can use the script *sysinit/futures/seed_price_data_from_csi.py* to initialize contract price data for either single instruments or all instruments available from CSI. 

After this you should of course create roll calendar for each instrument and then create multiple and adjusted price series as instructed [here](https://github.com/robcarver17/pysystemtrade/blob/master/docs/data.md#roll-calendars)

### Automating daily data updates from CSI Data CSV files

For updating prices manually you can use the script *sysproduction/update_historical_prices_from_csi.py* but for automating the process you can insert following section into your *private_config.yaml*:
```
historical_data_sources:
  IB:
    func: "sysproduction.update_historical_prices_from_ib.update_historical_prices"
    enabled: False
  CSI-data:
    func: "sysproduction.update_historical_prices_from_csv.update_historical_prices"
    enabled: True
    config:
      csv_config_factory_func: "sysdata.csi.csi_database.csv_config_factory"
      csv_datapath: "/your/shared/CSI/portfolio"
```
This will disable daily price updates from IB since every contract we might need is available on CSI anyway (this won't prevent downloading the FX data from IB though because that's a different process).  As CSI supplies only daily data the intraday prices would be missing. If you still would like to have intraday data for these instruments, we can configure pysystemtrade as below:

```
historical_data_sources:
  IB: 
    func: "sysproduction.update_historical_prices_from_ib.update_historical_prices"
    enabled: True
    config:
      # all available instruments are included by default
      frequency: 'H'  # hourly data only
  CSI-data:
    func: "sysproduction.update_historical_prices_from_csv.update_historical_prices"
    enabled: True
    config:
      csv_config_factory_func: "sysdata.csi.csi_database.csv_config_factory"
      csv_datapath: "/your/shared/CSI/portfolio"
```

## Historical data source configuration

As you've seen from the examples above the data sources can be configured quite easily and the user has fine-grained control over the parameters without having to alter the source code itself. 

The name of the data source is arbitrary and can be given any suitable name (it will be shown on log files). **Func** parameter is what actually determines what type of processor is used to handle updates from the data source. The **config** subsection is given as a parameter to the function and can as such contain any specific arguments applicable to the data source type (see below). The **enabled** setting can be set to 'False' if you want to disable a data source without having to delete the settings. A data source is enabled by default if this setting is omitted.

All data source types accept following general arguments in 'config' subsection:

- *include_instruments* : List of instruments. If this is omitted, ALL instruments are included by default. Having instrument listed here doesn't yet guarantee that it will be processed (all criteria listed in this section has to match as well)
- *exclude_instruments* : List of instruments. The include and exclude lists are processed in succession, so for blacklisting type rule you want to include ALL instruments and then list here those that you want to leave out.
- *frequency* : Either 'D' (daily), 'H' hourly or ALL. These can be given also as a list: ['D','H']
- *schedule* (discussed in more detail below)

In addition to these, each function can process its own specific arguments (such as csv_datapath when using CSV processor).

### Scheduling

With scheduling you can configure which instruments and data frequencies will be processed depending on the day of the week.  Scheduling is enabled with *'schedule'* keyword. Other general arguments listed above are ignored but instead are processed under each schedule entry separately.

A schedule accepts **days** setting, which can be given as a keyword ('weekend' or 'weekday') or as a list of days: [0,1,2,3,4] (Monday to Friday).

Here is an example of how one would configure Norgate to download all available daily data on weekdays and then augment the rest of the instrument data from IB.  On weekends we have time to download intraday data for all instruments for analyzing purposes for example. An exception would be the intraday prices for the volatile VIX and VSTOXX instruments, which we would like to fetch also during weekdays.

```
historical_data_sources:
  IB:
    func: "sysproduction.update_historical_prices_from_ib.update_historical_prices"
    config:
      schedule:
        my_weekday_schedule:  # the schedule name is arbitrary
          # all available instruments are included by default
          # Here we exclude all instruments that are available from Norgate
          exclude_instruments: ['AUD', 'BOBL', 'BTP', 'BUND', 'BUXL']
          # .. and many other instruments. For full list see the first Norgate example above

          # we want only daily price data
          frequency: D
          # this schedule is processed only on weekdays
          days: weekdays
        my_volatile_weekday_schedule:
          # .. but we want intraday (hourly) prices also on weekdays for these highly volatile instruments
          include_instruments: ['VIX','V2X']
          frequency: H
          days: [0,1,2,3,4] # same as 'weekdays'
        my_weekend_schedule:
          # On weekends we have time to fetch intraday and daily prices for all the instruments
          # No need to define instruments, all of them are included by default
          frequency: ALL  # also ['D','H'] could be used here
          days: weekend  # also [5,6] could be used here
  Norgate:
    func: "sysproduction.update_historical_prices_from_norgate.update_historical_prices"
    # No need for config - all available data will be downloaded anyway
```

### Contract price multiplier

For few rare instruments the price quotations Interactive Brokers provides have non-standard units (e.g. USD/lb) compared to the official contract specs listed on the exchanges web page (e.g. cents/lb) which other data sources (such as CSI and Norgate) use. In these cases to harmonize the prices we have to scale the data by multipling O/H/L/C values with **contract price multiplier** to get quotations in the same unit that IB provides and what is stored in Arctic.
   
For example instrument COPPER has contract size of 25000 lbs. Official units: cents/lb -> Full Point Value equals 250 USD. However IB quotations are reported as full dollars and IB multiplier is 25000. Thus we must set the multiplier to 0.01 so imported prices (cents/lb) are converted to USD/lb.

Another example is JPY for which IB quotations are in USD/YEN but official contract specs use USD/100 YEN so the multiplier in this case is also 0.01.

Norgate and CSI metadata database is augmented with contract price multiplier for each instrument that has a pysystemtrade instrument mapping. The functions handling price data from Norgate and CSI process this quotation scaling automatically, but if you are using a custom CSV handler you have to list the price multipliers separately for each instrument (see below for example).

**IMPORTANT:** If you are adding a new instrument please also make sure that the units match between IB and the data source you are using and if not, adjust the price multiplier. I also strongly advice you to double-check the price data and scaling for each instrument before you start trading them!


### Parametric CSV database

As you've seen from the CSI and Norgate CSV examples above, both data sources use the universal type CSV processor (Parametric CSV database) to read the files. The only differentiating factor is the **csv_datapath** which tells the processor where the files reside, and the **csv_config_factory_func** which configures the function that initializes a *ConfigCsvFuturesPrices* structure tailored specifically for this data source. This structure contains all the needed information the Parametric CSV database needs to process the CSV files, such as how files are named and laid out in the folder structure (filename format), the data broker symbol <-> instrument code mapping, CSV file column layout etc..

If you want to override some or all of the default csv config factory settings, you can do that by adding entries under the **csv_config** subsection (see example below).

For more information about the config structure and the formatting codes, please take a look at the *sysdata/csv/parametric_csv_database.py* module. 

If you want to add a new data provider that also uses CSV files, you could create a new CSV config factory function that sets up the ConfigCsvFuturesPrices structure correctly.  Alternatively if you just want to experiment with few instruments you could give all the needed information in csv_config subsection without using the config factory at all:

```
historical_data_sources:
  Barchart:
    func: "sysproduction.update_historical_prices_from_csv.update_historical_prices"
    enabled: True
    config:
      csv_datapath: "/your/barchart/directory"
      csv_config:
        input_date_index_name: "Time"
        input_skipfooter: 1
        input_date_format: "%m/%d/%Y"
        input_column_mapping:
          OPEN: "Open"
          HIGH: "High"
          LOW: "Low"
          FINAL: "Last"
          VOLUME: "Volume"
        input_filename_format: "%{BS}%{LETTER}%{YEAR2}_%{IGNORE}.csv"
        instrument_price_multiplier:
          JPY: 0.01
        broker_symbols:
          JPY: "j6"
          CRUDE_W_mini: "qm"
```

