"""
    This script will use Norgate Python API to download contracts for single or all instruments 
    and save them into Arctic
"""
from sysdata.norgate.norgate_futures_per_contract_prices import norgateFuturesContractPriceData, futuresContract
from sysdata.arctic.arctic_futures_per_contract_prices import arcticFuturesContractPriceData

def seed_price_data_from_NG(instrument_code):

    ng_prices = norgateFuturesContractPriceData()
    arctic_prices = arcticFuturesContractPriceData()

    ng_price_dict = ng_prices.get_all_prices_for_instrument(instrument_code)

    print("Have prices for the following %s contracts:" % instrument_code)
    print(str(ng_price_dict.keys()))

    i = 0
    for contract_date_str, prices_for_contract in ng_price_dict.items():
        i += 1
        print("Processing %s : %s  (contract %d/%d)" \
            % (instrument_code, contract_date_str, i, len(ng_price_dict)))

        contract = futuresContract(instrument_code, contract_date_str)
        print("Norgate prices are \n %s" % str(prices_for_contract))
        print("     Contract object is %s" % str(contract))

        print("Writing to arctic")
        arctic_prices.write_prices_for_contract_object(contract, prices_for_contract, ignore_duplication=True)

        print("Reading back prices from arctic to check")
        written_prices = arctic_prices.get_prices_for_contract_object(contract)
        print("Read back prices are \n %s" % str(written_prices))


def get_available_instruments(): 
    ng_prices = norgateFuturesContractPriceData()
    instruments = ng_prices.get_list_of_instrument_codes_with_price_data()
    instruments.sort()
    return instruments


def seed_all_price_data_from_NG():
    instruments = get_available_instruments()
    instr_index = 0
    for instr in instruments:
        instr_index += 1
        print("Processing %s (instrument %d/%d)" \
            % (instr, instr_index, len(instruments)))
        seed_price_data_from_NG(instr)


if __name__ == "__main__":
    print("Get initial price data from Norgate")
    instruments = get_available_instruments()
    print("Available instruments: ", instruments)
    
    instrument_code = input("Instrument code (or 'ALL')? <return to abort> ")
    if instrument_code == "":
        exit()

    if instrument_code != "ALL":
        seed_price_data_from_NG(instrument_code)
    else:
        seed_all_price_data_from_NG()