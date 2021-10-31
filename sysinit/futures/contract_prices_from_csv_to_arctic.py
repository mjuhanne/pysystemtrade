import collections
from syscore.objects import arg_not_supplied

from sysdata.csv.csv_futures_contract_prices import csvFuturesContractPriceData
from sysdata.arctic.arctic_futures_per_contract_prices import (
    arcticFuturesContractPriceData,
)
from sysobjects.contracts import futuresContract
from sysdata.data_blob import dataBlob

# Keeping the csvFuturesContractPriceData inside datablob means that we
# don't have to scan the folder tree and re-create the data structure containing 
# possibly tens of thousands contracts, again and again for every iterated instrument
def create_arctic_csv_datablob(
	datapath: str, csv_config = arg_not_supplied,
    name = "Init-arctic-with-csv-futures-contract-prices"
):
    data = dataBlob(log_name=name, 
                class_list=[arcticFuturesContractPriceData, csvFuturesContractPriceData ],
                csv_configs={"csvFuturesContractPriceData":csv_config},
                csv_data_paths={"csvFuturesContractPriceData":datapath},
                keep_original_prefix=True)
    return data

def init_arctic_with_csv_futures_contract_prices(
	datapath: str, csv_config=arg_not_supplied
):
    data = create_arctic_csv_datablob(datapath, csv_config)
    init_arctic_with_csv_datablob(data)


def init_arctic_with_csv_datablob(data: dataBlob):
    input("WARNING THIS WILL ERASE ANY EXISTING ARCTIC PRICES WITH DATA FROM %s ARE YOU SURE?! (CTRL-C TO STOP)" % csv_prices.datapath)

    instrument_codes = data.csv_futures_contract_price.get_list_of_instrument_codes_with_price_data()
    instrument_codes.sort()
    for instrument_code in instrument_codes:
        init_arctic_with_csv_datablob_for_code(
        	instrument_code, data
        )


def init_arctic_with_csv_futures_contract_prices_for_code(
	instrument_code: str, datapath: str, csv_config=arg_not_supplied
):
    data = create_arctic_csv_datablob(datapath, csv_config)
    init_arctic_with_csv_datablob_for_code(instrument_code, data)


def init_arctic_with_csv_datablob_for_code(
	instrument_code: str, data: dataBlob
):
    print(instrument_code)

    print("Getting .csv prices may take some time")
    csv_price_dict = data.csv_futures_contract_price.get_all_prices_for_instrument(instrument_code)
    csv_price_dict = collections.OrderedDict(sorted(csv_price_dict.items()))

    print("Have .csv prices for the following contracts:")
    print(str(csv_price_dict.keys()))

    for contract_date_str, prices_for_contract in csv_price_dict.items():
        print("Processing %s" % contract_date_str)
        print(".csv prices are \n %s" % str(prices_for_contract))
        contract = futuresContract(instrument_code, contract_date_str)
        print("Contract object is %s" % str(contract))
        print("Writing to arctic")
        data.arctic_futures_contract_price.write_prices_for_contract_object(
        	contract, prices_for_contract, ignore_duplication=True
        )
        print("Reading back prices from arctic to check")
        written_prices = data.arctic_futures_contract_price.get_prices_for_contract_object(contract)
        print("Read back prices are \n %s" % str(written_prices))


if __name__ == "__main__":
    input("Will overwrite existing prices are you sure?! CTL-C to abort")
    # modify flags as required
    datapath = "*** NEED TO DEFINE A DATAPATH***"
    init_arctic_with_csv_futures_contract_prices(datapath)
