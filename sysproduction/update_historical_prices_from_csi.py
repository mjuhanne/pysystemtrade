"""
Script to manually update historical data per contract from CSV files created by CSI Data's Unfair Advantage and dump into mongodb

Note that automatic updates are configured in private_config.yaml 
"""
from sysdata.data_blob import dataBlob
from sysproduction.data.prices import diagPrices
from sysdata.config.configdata import Config
from sysproduction.update_historical_prices import DATASOURCE_CONFIG
from sysproduction.update_historical_prices_from_csv import updateHistoricalPricesCsv
from sysdata.csi.csi_database import csv_config_factory

csv_datapath = "/your/shared/CSI/datafolder"


if __name__ == "__main__":
    print("Update price data from CSI")

    csv_config = csv_config_factory()

    data_historical = dataBlob(log_name="Update-Historical-Prices-CSI")

    # If we want to modify parametric CSV database settings we could of course alter the csv_config 
    # structure directly here, but alternative way is to do it via Config dict 
    # (as it is done in private_config.yaml when we would like to overwrite config factory settings by
    # declaring 'csv_config' entry there)
    config = Config(
        dict( 
            config=dict(
                csv_config=dict( verbose_scanning=False )
                )
            )
        )
    config_item = config.get_element_or_missing_data(DATASOURCE_CONFIG) # 'config'

    obj = updateHistoricalPricesCsv(data=data_historical, datasource="CSI", 
        config=config_item, csv_datapath=csv_datapath, csv_config=csv_config )
        
    instruments = obj.get_data_broker().get_list_of_instrument_codes_with_price_data()

    instruments.sort()
    print("Available instruments from CSI-data: ", instruments)

    price_data = diagPrices(data_historical)
    mult_instruments = price_data.get_list_of_instruments_in_multiple_prices()
    print("\nInstruments containing multiple prices: ", mult_instruments)

    print("\nEnter instrument code. ")
    print("Alternatively enter 'ALL' for updating prices for all instruments or")
    print("'MULT' for updating only those instruments containing multiple prices.")
    instrument_code = input("Selection: <return to abort> ")
    if instrument_code == "":
        exit()

    if instrument_code == "ALL":
        for instr in instruments:
            obj.update_historical_prices_for_instrument(instr, data_historical)
    else:
        if instrument_code == 'MULT':
            obj.update_historical_prices()
        else:
            obj.update_historical_prices_for_instrument(instrument_code, data_historical)
