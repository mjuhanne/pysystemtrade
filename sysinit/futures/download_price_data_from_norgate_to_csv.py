"""
    This script uses Norgate Python API to download contracts from Norgate for single 
    or all instruments and save them into target .csv files. 
"""

from sysdata.norgate.norgate_futures_per_contract_prices import norgateFuturesContractPriceData, futuresContract
from sysdata.csv.csv_futures_contract_prices import csvFuturesContractPriceData
import os

ng_prices = norgateFuturesContractPriceData()

# insert here instrument codes which you want to ignore to download
# even when using 'ALL' keyword to fetch all instruments
ignore_instruments = []

def seed_price_data_from_NG(instrument_code, csv_datapath):

    list_of_contracts = ng_prices.contract_dates_with_price_data_for_instrument_code(instrument_code)
    print(instrument_code," contracts: ", list_of_contracts)

    # create a subdirectory for the instrument
    try: 
        os.makedirs(csv_datapath)
    except FileExistsError:
        print(csv_datapath, " already exists")

    for contract_date in list_of_contracts:
        contract = futuresContract(instrument_code, contract_date)
        seed_price_data_for_contract(contract, instrument_code, csv_datapath)


def seed_price_data_for_contract( contract: futuresContract, instrument_code, csv_datapath):
    date_str = contract.contract_date.date_str[:6]
    new_contract = futuresContract(contract.instrument,
                                   date_str)

    csv_prices = csvFuturesContractPriceData(datapath=csv_datapath)

    prices = ng_prices.get_prices_at_frequency_for_potentially_expired_contract_object(new_contract,
        include_open_interest=False)
    if len(prices)==0:
        print("Warning! No data for contract ",date_str)
    else:
        print("Writing contract ", instrument_code, " : ", date_str)
        csv_prices.write_prices_for_contract_object(new_contract, prices, ignore_duplication=False)


def seed_all_price_data_from_NG( csv_datapath, all_instruments ):

    for instrument_code in all_instruments:
        if instrument_code in ignore_instruments:
            print("**** Skipping instrument: ", instrument_code," ****")
        else:
            seed_price_data_from_NG(instrument_code, csv_datapath + "/" + instrument_code)


def get_available_instruments(): 
    instruments = ng_prices.get_list_of_instrument_codes_with_price_data()
    instruments.sort()
    return instruments


if __name__ == "__main__":

    csv_datapath = "****** NEEDS TARGET DATAPATH TO BE CONFIGURED ****"

    instruments = get_available_instruments()
    print("Available Norgate instruments: ", instruments)

    instrument_code = input("Instrument code? (enter 'ALL' for selecting all instruments) <return to abort> ")
    if instrument_code == "":
        exit()

    if (instrument_code == "ALL"):
        seed_all_price_data_from_NG(csv_datapath, instruments)
    else:
        seed_price_data_from_NG(instrument_code, csv_datapath + "/" + instrument_code)

