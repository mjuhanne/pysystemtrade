"""
Update historical data per contract from CSV files and dump into mongodb
"""
from sysdata.data_blob import dataBlob
from syscore.objects import success, failure, arg_not_supplied
from syscore.dateutils import DAILY_PRICE_FREQ, Frequency
from syscore.objects import missing_instrument
from sysdata.csv.csv_futures_contract_prices import csvFuturesContractPriceData, futuresContract
from sysproduction.update_historical_prices_base import updateHistoricalPricesBase, ALL_INSTRUMENTS
from sysdata.futures.futures_per_contract_prices import futuresContractPriceData
from syscore.objects import resolve_function
from sysdata.csv.parametric_csv_database import ConfigCsvFuturesPrices

CSV_CONFIG_FACTORY_FUNC = "csv_config_factory_func"
CSV_DATAPATH = "csv_datapath"
CSV_CONFIG = "csv_config"

def update_historical_prices(data:dataBlob = arg_not_supplied, datasource:str ="CSV", instrument_code:str = ALL_INSTRUMENTS, 
    manual_price_check:bool = False, config = arg_not_supplied ):
    """
    Do a daily update for futures contract prices, using historical data from CSV files
    :param data dataBlob
    :param datasource:str Name of this datasource
    :param instrument_code:str Instrument for which prices are to be updated or 'ALL'
    :param manual_price_check:bool If true, instead of reporting price spikes we run manual price checking 
    :param config Yaml configuration entry that has csv_datapath and csv_config_factory_func specified
    :return: Nothing
    """
    try:
        csv_datapath = config[CSV_DATAPATH]
        # Get the csv_config from factory that is specified for this particular CSV source (e.g. CSI data or Norgate)
        # csv_config has all the data that is needed to properly read the files
        # (broker symbol <-> instrument code mapping, unit multipliers, file name format etc)
        if CSV_CONFIG_FACTORY_FUNC in config:
            func_name = config[CSV_CONFIG_FACTORY_FUNC]
            try:
                func = resolve_function(func_name)
                csv_config = func()
            except Exception as e:
                print("Error executing csv config factory function %s for datasource %s" % (func_name,datasource))
                raise
        else:
            # CSV config factory is not defined. We then need at least csv_config section
            if CSV_CONFIG not in config:
                raise Exception("Datasource %s has neither CSV config factory nor csv_config defined!" % datasource)
            csv_config = arg_not_supplied
    except:
        print("Error in datasource %s configuration!" % datasource)
        raise
    
    if data is arg_not_supplied:
        data = dataBlob(log_name="Update-Historical-Prices-%s" % datasource )

    update_historical_price_object = updateHistoricalPricesCsv(data=data,
        datasource=datasource, config=config, csv_datapath=csv_datapath, 
        csv_config=csv_config )
    update_historical_price_object.update_historical_prices(instrument_code, manual_price_check=manual_price_check)
    return success


class updateHistoricalPricesCsv(updateHistoricalPricesBase):
    def __init__(self, data, datasource:str = "CSV", config = arg_not_supplied,
        csv_datapath:str = arg_not_supplied, csv_config:ConfigCsvFuturesPrices = arg_not_supplied ):

        super().__init__(data, datasource, config)
        _csv_config = csv_config
        if self.config is not arg_not_supplied:
            _csv_config = self._update_csv_config(_csv_config)
        self.data_broker = csvFuturesContractPriceData(datapath = csv_datapath, config = _csv_config)

    def get_data_broker(self) -> futuresContractPriceData:
        return self.data_broker


    def _update_csv_config(self, csv_config:ConfigCsvFuturesPrices):
        """
        If available, updates the csv_config structure from the Config 'csv_config' subconfiguration 
        associated with this data source. Can be handy when overwriting settings retrieved 
        originally from csv_config factory
        """
        # If csv_config is not given, initialize it now with default values...
        if csv_config is arg_not_supplied:
            csv_config = ConfigCsvFuturesPrices()
        # .. then update with values from csv_config if available
        if CSV_CONFIG in self.config:
            for item_name in self.config[CSV_CONFIG]:
                setattr(csv_config, item_name, self.config[CSV_CONFIG][item_name])
        return csv_config


    def update_historical_prices_for_instrument(self, instrument_code: str, data: dataBlob):
        """
        Do a daily update for futures contract prices, using historical data from parametric CSV database

        :param instrument_code: str
        :param data: dataBlob
        :return: None
        """
        data_broker = self.get_data_broker()
        contract_list = data_broker.contracts_with_price_data_for_instrument_code(instrument_code,
            allow_expired=False)

        if contract_list is missing_instrument:
            print("Prices for instrument", instrument_code, " is not provided by", self.datasource)
            return failure

        for contract_object in contract_list:
            data.log.label(contract_date = contract_object.date_str)
            self.update_historical_prices_for_instrument_and_contract(
                contract_object, data)

        return success


    def update_historical_prices_for_instrument_and_contract(
            self, contract_object: futuresContract, data: dataBlob):
        """
        Do a daily update for futures contract prices, using historical data from parametric CSV database

        :param contract_object: futuresContract
        :param data: data blob
        :return: None
        """

        # Get only daily data
        result = self.get_and_add_prices_for_frequency(
            data, contract_object, frequency=DAILY_PRICE_FREQ)
        
        return result
