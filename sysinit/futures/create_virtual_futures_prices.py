from sysdata.arctic.arctic_adjusted_prices import arcticFuturesAdjustedPricesData
from sysdata.arctic.arctic_multiple_prices import arcticFuturesMultiplePricesData
from sysdata.arctic.arctic_futures_per_contract_prices import arcticFuturesContractPriceData
from sysdata.data_blob import dataBlob
from sysdata.futures.virtual_futures_data import virtualFuturesData
from sysdata.mongodb.mongo_futures_instruments import mongoFuturesInstrumentData
from sysbrokers.IB.ib_futures_contract_price_data import ibFuturesContractPriceData
from sysbrokers.IB.ib_futures_contracts_data import ibFuturesContractData
from sysinit.futures.seed_price_data_from_IB import seed_price_data_from_IB
import datetime

data = dataBlob()
data.add_class_list(
    [
        ibFuturesContractPriceData,
        arcticFuturesContractPriceData,
        ibFuturesContractData,
    ]
)

def create_virtual_futures_prices(instrument_code):

    if not virtualFuturesData.is_virtual(instrument_code):
        print(instrument_code," is not virtual futures instrument!")

    prices = virtualFuturesData.get_prices(data, instrument_code)
    if (len(prices) == 0):
        print("No prices for",instrument_code,". Downloading from IB..")
        try:
            seed_price_data_from_IB(instrument_code, data)
        except BaseException as exp:
            print("Error downloading prices (%s)! Omitting instrument" % (exp))
            return


def create_all_virtual_futures_prices():
    mongo = mongoFuturesInstrumentData()
    instruments = mongo.get_list_of_instruments()
    for instr in instruments:
        if virtualFuturesData.is_virtual(instr):
            print("Processing",instr)
            create_virtual_futures_prices(instr)


if __name__ == "__main__":
    print("Create dummy prices for virtual futures instrument from stock prices")
    instrument_code = input("Instrument code? or 'ALL' <return to abort> ")
    if instrument_code == "":
        exit()

    if instrument_code == "ALL":
        create_all_virtual_futures_prices()
    else:
        create_virtual_futures_prices(instrument_code)
