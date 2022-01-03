"""
CSI instrument database and CSV config factory

IMPORTANT: When importing a portfolio to CSI's Unfair Advantage please make sure that it is configured as follows:
- Edit Porftolio 
    -> "ASCII/Excel files" tab  
        - Name By: "#_S_CY_A" without quotes
        - Use generic filename override for continuous series: CHECKED (if you want to fiddle with precalculated adjusted/continuous contracts)
        - Export Futures into Symbolized Directories: CHECKED  (so you don't have circa 25000 contracts in same directory.. :)
    -> "ASCII/Excel fields" tab
        - Fields: "DOHLCv" without quotes. (there are many others such as open interest, expiration date, spot pricing etc)
        - Separator: comma
        - Date separator: none
        - Date Format: YYYY/MM/DD

IMPORTANT #2: Please get the full market data file from Unfair Advantage yourself (see below)
"""
from numpy import NaN
import pandas as pd

from syscore.objects import missing_instrument
from syscore.fileutils import get_filename_for_package
from sysdata.csv.csv_futures_contract_prices import ConfigCsvFuturesPrices
 
"""
These are examples of contract price unit multipliers (now configured in csi-config.csv). When price unit differs between IB and CSI we
must multiply CSI prices with these multipliers so that price magnitude is coherent with price data downloaded from IB and saved in Arctic.

contract_multipliers = { 
                        "KRWUSD"    : 0.001,  # USD / 1000 TWD
                        "TWD"       : 0.001,  # USD / 1000 TWD
                        "SILVER"    : 0.01,   # USD cents / troy oz
                        "COPPER"    : 0.01,   # USD cents / lb
                        "HUF"       : 0.01,   # USD / 100 Hungarian forint
                        "CZK"       : 0.01,   # USD / 100 Czech koruna
                        "JPY"       : 0.01    # UDS / 100 Japanese yen
                        }  
"""

# This is a config file containing instrument code <-> CSI Id mapping, unit multipliers as well as "Enabled" setting
# which is used to select wanted instruments into Unfair Advantage portfolio (a list of instruments/contracts that we want to follow)
CSI_CONFIG_FILE = get_filename_for_package("sysdata.csi.csi-config.csv")

# This is a full market file saved as a CSV file from Unfair Advantage.
#
# It contains a list of circa 1400 instruments and includes many fields that the 
# fact sheets below don't, namely Futures category, Last price, price change %, avg yearly volume
# What it however doesn't contain is full point value (which could be calculated though from other fields 
# but it's a bit tricky so it's preferred to use the precalculated value from fact sheets below)
# 
# It isn't distributed in this Github repository for legal reasons, but when you have CSI subscription 
# you can create the file yourself using Unfair Advantage:
#   Main menu -> Market specs -> Check 'Include Latest Pricing Information' 
#      -> File -> Save table results to file
# 
# The file isn't strictly required for downloading latest price data if you want to use the example portfolio,
# but it is needed if you want to create your own custom porfolio with porfolio constructor or validate
# instrument database after adding new instruments. You don't need to update it very often though
# (maybe once per year?) because the price and volume data is used just for screening purposes and it isn't
# accessed after you have set up your portfolio
MARKETS_FILE = get_filename_for_package("sysdata.csi.markets.csv")
MARKET_FILE_NOT_FOUND_WARNING = "CSI market data file not found! Full instrument metadata not available!"

# CSI Data fact sheets per exchange, downloaded as CSV files from https://apps.csidata.com/FactsheetListing.aspx
# As opposed to the market data above, these contain precalculated full point value for each instrument
factsheet_exchange_files = { "ASX.csv", "CFE.csv", "CFTC-COT.csv",
    "CME.csv", "EUREX.csv", "EURONEXT.csv", "HKEX.csv", "ICE.csv",
    "KRX.csv", "OSE.csv", "SGX.csv" }
FACTSHEET_FILES = [ get_filename_for_package("sysdata.csi.exch." + filename) for filename in factsheet_exchange_files]


CSI_ID_COLUMN = "CSI"
ID_COLUMN = "Id"
ENABLED_COL = "Enabled"
UNIT_MULTIPLIER_COL = "UnitMultiplier"
OUR_DESC_COL = "OurDescription"
CSI_DESC_COL = "CSI_Description"
EXCH_COL = "Exchange"
CURRENCY_COL = "Currency"
UNITS_COL = "Units"
FULL_POINT_VALUE_COL = "FullPointValue"

LAST_CLOSE_COL = "Last Close"
AVG_VOLUME_COL = "Avg Volume (Year)"
LAST_TOTAL_VOLUME_COL = "LastTotalVolume"

FACTSHEET_CSI_ID_COLUMN = "UACsiNumber"

NAME_COL = "Name"
SHORT_NAME_COL = "ShortName"
EXCH_SYMBOL_COL = "Exch Symbol"
IS_ACTIVE_COL = "IsActive"
SESSION_TYPE_COL = "SessionType"
CONTRACT_VALUE = "ContractValue"
USD_CONTRACT_VALUE = "USDContractValue"

# This is the directory path and filename structure for CSV files
DEFAULT_CSI_FILENAME_FORMAT = "%{BS}_%{IGNORE}/%{BS}_%{IGNORE}_%{YEAR}_%{LETTER}.csv"
DEFAULT_CSI_CONTINUOUS_CONTRACT_FILENAME_FORMAT = "%{BS}_%{IGNORE}/%{IGNORE}_B.csv"


# CSV config factory for the parametric CSV database. This sets the default parameters for .csv files created 
# by Unfair Advantage so they can be read by csvFuturesContractPriceData.
def csv_config_factory():
    csi_db = csiInstrumentDatabase()

    csv_config = ConfigCsvFuturesPrices()
    csv_config.input_filename_format = DEFAULT_CSI_FILENAME_FORMAT
    csv_config.input_date_format = "%Y%m%d"
    input_column_mapping = dict(OPEN='Open',
                            HIGH='High',
                            LOW='Low',
                            FINAL='Close',
                            VOLUME='Volume'
                            )
    csv_config.input_date_index_name = "Date"
    csv_config.input_column_mapping = input_column_mapping                                                    
    csv_config.broker_symbols = csi_db.get_id_map()
    csv_config.instrument_price_multiplier = csi_db.get_unit_multipliers()
    return csv_config


class csiInstrumentDatabase(object):

    def __init__(self):
        # Load instrument code <-> 
        self.config = pd.read_csv(CSI_CONFIG_FILE)

        try:
            # load Unfair Advantage's market data file
            markets_data = pd.read_csv(MARKETS_FILE)

            # load all factsheets (1 per exchange)
            factsheet = None
            for factsheet_file in FACTSHEET_FILES:
                factsheet_exch = pd.read_csv(factsheet_file)
                if factsheet is None:
                    factsheet = factsheet_exch
                else:
                    factsheet = \
                        factsheet.append(factsheet_exch, ignore_index=True)
            factsheet.rename(
                columns = { 
                    FACTSHEET_CSI_ID_COLUMN: CSI_ID_COLUMN, 
                    NAME_COL : SHORT_NAME_COL,
                    EXCH_COL : "Exchange2", 
                    CURRENCY_COL : "Currency2", 
                    UNITS_COL : "Units2" },
                inplace=True)

            # join factsheets with the market file so that all metadata can be accessed from a single row
            self.markets = markets_data.join(factsheet.set_index(CSI_ID_COLUMN), on=CSI_ID_COLUMN )
        except:
            print(MARKET_FILE_NOT_FOUND_WARNING)
            self.markets = None


    def is_enabled(self, instrument:str):
        res = self.config[ self.config[ID_COLUMN] == instrument]
        if len(res)==0:
            return 0
        return int(res.iloc[-1].at[ENABLED_COL])


    def get_csi_id(self, instrument:str):
        res = self.config[ self.config[ID_COLUMN] == instrument]
        if len(res)==0:
            return missing_instrument
        if pd.isnull(res.iloc[-1].at[CSI_ID_COLUMN]):
            return missing_instrument
        return str(int(res.iloc[-1].at[CSI_ID_COLUMN])) # drop annoying trailing .0

    def get_csi_instrument_metadata(self, csi_id:str) -> pd.Series:
        if self.markets is None:
            raise Exception(MARKET_FILE_NOT_FOUND_WARNING)
        res = self.markets[ self.markets[CSI_ID_COLUMN] == int(csi_id)]
        if len(res)==0:
            return missing_instrument
        return res.iloc[-1]

    def get_list_of_csi_instruments(self):
        if self.markets is None:
            raise Exception(MARKET_FILE_NOT_FOUND_WARNING)
        return self.markets[CSI_ID_COLUMN].to_list()

    def get_unit_multiplier(self, instrument:str):
        res = self.config[ self.config[ID_COLUMN] == instrument]
        if len(res)==0:
            return missing_instrument
        return res.iloc[-1].at[UNIT_MULTIPLIER_COL]

    def get_unit_multipliers(self):
        m = dict()
        ids =  self.config[ self.config[CSI_ID_COLUMN] == self.config[CSI_ID_COLUMN] ]
        ids =  ids[ ids[ID_COLUMN] == ids[ID_COLUMN] ]
        for index, row in ids.iterrows():
            multiplier = row[UNIT_MULTIPLIER_COL]
            id = row[ID_COLUMN]
            m[ id ] = multiplier
        return m

    def get_instrument_code(self, csi_id:str) -> pd.Series:
        res = self.config[ self.config[CSI_ID_COLUMN] == csi_id]
        if len(res)==0:
            return missing_instrument
        if res.iloc[-1].at[ID_COLUMN] is NaN:
            return missing_instrument
        return res.iloc[-1].at[ID_COLUMN]

    def get_id_map(self) -> dict:
        m = dict()
        # get all csi ids / instrument code pairs 
        ids =  self.config[ self.config[CSI_ID_COLUMN] == self.config[CSI_ID_COLUMN] ]
        ids =  ids[ ids[ID_COLUMN] == ids[ID_COLUMN] ]
        for index, row in ids.iterrows():
            csi_id = str(int(row[CSI_ID_COLUMN]))
            id = row[ID_COLUMN]
            m[ id ] = csi_id
        return m
