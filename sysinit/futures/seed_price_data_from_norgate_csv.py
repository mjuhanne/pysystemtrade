"""
    This script scans a target directory containing .csv files created by Norgate Date Updater.
    It will then load the available contracts for single or all instruments 
    and save them into Arctic
"""
from contract_prices_from_csv_to_arctic import *
from sysdata.norgate.ng_database import csv_config_factory
from sysdata.data_blob import dataBlob

if __name__ == "__main__":

    csv_datapath = "****** NEEDS SOURCE DATAPATH TO BE CONFIGURED ****"

    # creates CSV config structure tailored for reading Norgate CSV files
    csv_config = csv_config_factory()

    # datablob containing parametric CSV database (csvFuturesContractPriceData) 
    # and arcticFuturesContractPriceData 
    data = create_arctic_csv_datablob(csv_datapath, csv_config,"Seed-price-data-from-Norgate-CSV")

    print("Get initial price data from Norgate")
    instruments = data.csv_futures_contract_price.get_list_of_instrument_codes_with_price_data()
    instruments.sort()
    print("Available instruments: ", instruments)
    
    instrument_code = input("Instrument code (or 'ALL')? <return to abort> ")
    if instrument_code == "":
        exit()

    if instrument_code != "ALL":
        init_arctic_with_csv_datablob_for_code(instrument_code, data)
    else:
        init_arctic_with_csv_datablob(data)