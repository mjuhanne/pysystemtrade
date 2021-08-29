"""
Script to manually ipdate historical data per contract from Norgate CSV files and dump into mongodb

Note that automatic updates are configured in private_config.yaml 
"""
from sysdata.data_blob import dataBlob
from sysproduction.data.prices import diagPrices
from sysproduction.update_historical_prices_from_csv import updateHistoricalPricesCsv
from sysdata.norgate.ng_database import csv_config_factory

csv_datapath = "/your/shared/folder/for/Norgate/Futures"


if __name__ == "__main__":
    print("Update price data from Norgate CSV files")

    csv_config = csv_config_factory()

    # we have so many unmapped symbols so let's prevent flooding the log files..
    csv_config.ignore_missing_symbols = 'ALL'

    data_historical = dataBlob(log_name="Update-Historical-Prices-Norgate-CSV")

    obj = updateHistoricalPricesCsv(data=data_historical, datasource="NorgateCSV", 
        csv_datapath=csv_datapath, csv_config=csv_config )

    instruments = obj.get_data_broker().get_list_of_instrument_codes_with_price_data()

    instruments.sort()
    print("Available instruments from Norgate: ", instruments)

    price_data = diagPrices(data_historical)
    mult_instruments = price_data.get_list_of_instruments_in_multiple_prices()
    mult_instruments = [value for value in mult_instruments if value in instruments]
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
