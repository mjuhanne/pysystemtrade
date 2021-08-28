"""
This script will delete (some or all) contract prices for given instrument
"""
from sysobjects.contracts import futuresContract
from sysdata.arctic.arctic_futures_per_contract_prices import (
    arcticFuturesContractPriceData,
)
from sysproduction.data.prices import get_valid_instrument_code_from_user

    
def delete_contract_prices_for_instrument(instrument_code:str):
    prices = arcticFuturesContractPriceData()
    contracts = prices.contracts_with_price_data_for_instrument_code(instrument_code)
    contract_dates = contracts.contract_date_str_for_contracts_in_list_for_instrument_code(instrument_code)
    contract_dates.sort()
    print("Available contracts: ", contract_dates)

    cdate = input("Enter contract date or ALL if you want to delete all instrument contracts: ")
    if cdate=='ALL':
        print("Please confirm that you want to delete ALL contracts for instrument ", instrument_code)
    else:
        print("Please confirm that you want to delete contract",cdate," for instrument ", instrument_code)

    confirmation = input("Enter YES for confirmation: ")
    if confirmation=='YES':
        print("Deleting contract(s)..")
        if cdate=='ALL':
            try:
                prices.delete_all_prices_for_instrument_code(instrument_code, areyousure=True)
            except:
                print("Could not delete prices for instrument ", instrument_code)
        else:
            try:
                fc = futuresContract.from_two_strings(instrument_code, cdate)
                prices.delete_prices_for_contract_object(fc, areyousure=True)
            except:
                print("Could not delete contract ", cdate)
    else:
        print("Prices NOT deleted")


if __name__ == "__main__":
    print("WARNING! This is a script for deleting contracts from Arctic database! CTRL-C to abort")
    instrument_code = get_valid_instrument_code_from_user(source='single')
    delete_contract_prices_for_instrument(instrument_code)
