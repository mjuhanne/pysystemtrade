"""
Updates historical data per contract and dumps into mongodb

It will read the "historical_data_sources" section in defaults.yaml or private_config.yaml and update
historical data from all the configured and enabled data sources.

This script will be called by run_daily_price_updates.py and interactive_manual_check_historices_prices.py
but can also be executed manually to update prices on all or some instruments
"""
from syscore.objects import success, failure, missing_data, arg_not_supplied
from syscore.objects import resolve_function

from sysdata.data_blob import dataBlob
from sysproduction.data.prices import diagPrices
from sysdata.config.production_config import get_production_config
from sysproduction.update_historical_prices_base import ALL_INSTRUMENTS
from sysproduction.update_historical_prices_from_ib import updateHistoricalPricesIB

HISTORICAL_DATA_SOURCES = "historical_data_sources"
DATASOURCE_ENABLED = "enabled"
DATASOURCE_FUNC = "func"
DATASOURCE_CONFIG = "config"

production_config = get_production_config()

def update_historical_prices():
    """
    Do a daily update for futures contract prices, using IB historical data

    :return: Nothing
    """
    with dataBlob(log_name="Update-Historical-Prices") as data:
        update_historical_price_object = updateHistoricalPrices(data)
        update_historical_price_object.update_historical_prices(instrument=ALL_INSTRUMENTS, manual_price_check=False)
    return success


class updateHistoricalPrices(object):
    def __init__(self, data):
        self.data = data

    def update_historical_prices(self, instrument=ALL_INSTRUMENTS, manual_price_check=False):

        # Data source iterator
        data_sources = production_config.get_element_or_missing_data(HISTORICAL_DATA_SOURCES)
        if data_sources is missing_data:
            self.data.log.warn("Historical data sources not configured. Defaulting to IB..")
            impl = updateHistoricalPricesIB(self.data)
            impl.update_historical_prices(instrument, manual_price_check)
        else:
            for datasource in data_sources:
                items = data_sources[datasource]
                print("Configuration for datasource %s: %s" % (datasource, items))
                if DATASOURCE_ENABLED in items:
                    enabled = items[DATASOURCE_ENABLED]
                else:
                    enabled = 1 # default

                if enabled:
                    if not DATASOURCE_FUNC in items:
                        raise Exception("Datasource %s is missing function!" % datasource)
                    func_name = items[DATASOURCE_FUNC]
                    if DATASOURCE_CONFIG in items:
                        config = items[DATASOURCE_CONFIG]
                    else:
                        config = arg_not_supplied
                    try:
                        func = resolve_function(func_name)
                        func( self.data, datasource, instrument_code=instrument, 
                            manual_price_check=manual_price_check, config=config )
                    except Exception as e:
                        self.data.log.error("Error executing function %s for datasource %s" % 
                            (func_name, datasource))
                        print(e)
                        raise
                else:
                    self.data.log.warn("Datasource %s is disabled" % datasource)


if __name__ == "__main__":
    print("Update price data")

    data = dataBlob(log_name="update_historical_prices")

    price_data = diagPrices(data)
    instruments = price_data.get_list_of_instruments_in_multiple_prices()
    print("Available instruments containing multiple prices: ", instruments)

    print("Enter instrument code (or ALL)")
    instrument_code = input("Selection: <return to abort> ")
    if instrument_code == "":
        exit()

    obj = updateHistoricalPrices(data)

    if instrument_code == ALL_INSTRUMENTS:
        obj.update_historical_prices()
    else:
        obj.update_historical_prices(instrument_code)
