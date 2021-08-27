"""
Norgate instrument database. This contains mapping of Norgate instrument ID -> instrument code, as well as other interesting metadata
of each instrument. 
"""
from numpy import NaN
import pandas as pd

from syscore.objects import missing_instrument
from syscore.fileutils import get_filename_for_package
from sysdata.csv.csv_futures_contract_prices import ConfigCsvFuturesPrices

NG_CONFIG_FILE = get_filename_for_package("sysdata.norgate.ng_config_futures.csv")

NG_ID_COLUMN = "NorgateInstrument"
ID_COLUMN = "Instrument"
EXCH_COL = "Exchange"
CURRENCY_COL = "Currency"
DESC_COL = "Name"
POINTVALUE_COL = "PointValue"
UNIT_MULTIPLIER_COL = "UnitMultiplier"


# CSV config factory for parametric CSV database. This can be used with csvFuturesContractPriceData if you want to
# load prices from CSV files that are saved by Norgate Date Updater (configured via its Export Task Manager)
def csv_config_factory():
    db = norgateInstrumentDatabase()

    csv_config = ConfigCsvFuturesPrices()
    csv_config.input_filename_format = "%{BS}-%{YEAR}%{LETTER}.csv"
    csv_config.input_date_format = "%Y%m%d"
    input_column_mapping = dict(OPEN='Open',
                            HIGH='High',
                            LOW='Low',
                            FINAL='Close',
                            VOLUME='Volume'
                            )
    csv_config.input_date_index_name = "Date"
    csv_config.input_column_mapping = input_column_mapping                                                    
    csv_config.broker_symbols = db.get_id_map()
    csv_config.instrument_price_multiplier = db.get_unit_multipliers()
    return csv_config


class norgateInstrumentDatabase(object):

    def __init__(self):
        self.config = pd.read_csv(NG_CONFIG_FILE)

    def get_ng_id(self, instrument:str):
        """
        Gets Norgate instrument symbol
        :param instrument: str instrument code
        :return: Norgate instrument symbol or missing instrument
        """
        res = self.config[ self.config[ID_COLUMN] == instrument]
        if len(res)==0:
            return missing_instrument
        return res.iloc[-1].at[NG_ID_COLUMN]

    def get_ng_instrument_metadata(self, instrument:str) -> pd.Series:
        """
        Gets metadata for Norgate instrument (Pandas series)
        :param instrument: str instrument code
        :return: metadata as Pandas series or missing_instrument
        """
        ng_id = self.get_ng_id(instrument)
        if ng_id is missing_instrument:
            return missing_instrument

        res = self.config[ self.config[NG_ID_COLUMN] == ng_id]
        if len(res)==0:
            return missing_instrument
        return res.iloc[-1]

    def get_instrument(self, ng_id:str):
        """
        Gets instrument code by Norgate instrument symbol
        :param ng_id: str Norgate instrument symbol
        :return: instrument code or missing_instrument
        """
        res = self.config[ self.config[NG_ID_COLUMN] == ng_id]
        if len(res)==0:
            return missing_instrument
        if res.iloc[-1].at[ID_COLUMN] is NaN:
            return missing_instrument
        return res.iloc[-1].at[ID_COLUMN]

    def get_list_of_instruments(self) -> list:
        """
        Get instruments that have price data
        Pulls these in from a config file

        :return: list of str
        """
        res = self.config[ID_COLUMN]
        #remove NaN rows
        res = res[res==res]
        instrument_list = list(res)
        return instrument_list

    def get_unit_multiplier(self, instrument:str):
        """
        Gets multiplier that is used to process price data from Norgate 
        to match the price level that prices from Interactive Brokers have
        :param instrument: str instrument code
        :return: unit multiplier or missing_instrument
        """
        res = self.config[ self.config[ID_COLUMN] == instrument]
        if len(res)==0:
            return missing_instrument
        return res.iloc[-1].at[UNIT_MULTIPLIER_COL]

    def get_unit_multipliers(self):
        """
        Gets dict of unit multipliers, one for each instrument code. Multipliers are used to process 
        price data from Norgate to match the price level that prices from Interactive Brokers have
        :return: dict of unit multipliers
        """
        m = dict()
        ids =  self.config[ self.config[NG_ID_COLUMN] == self.config[NG_ID_COLUMN] ]
        ids =  ids[ ids[ID_COLUMN] == ids[ID_COLUMN] ]
        for index, row in ids.iterrows():
            multiplier = row[UNIT_MULTIPLIER_COL]
            id = row[ID_COLUMN]
            m[ id ] = multiplier
        return m

    def get_id_map(self) -> dict:
        m = dict()
        # get all csi ids / instrument code pairs 
        ids =  self.config[ self.config[NG_ID_COLUMN] == self.config[NG_ID_COLUMN] ]
        ids =  ids[ ids[ID_COLUMN] == ids[ID_COLUMN] ]
        for index, row in ids.iterrows():
            csi_id = row[NG_ID_COLUMN]
            id = row[ID_COLUMN]
            m[ id ] = csi_id
        return m
