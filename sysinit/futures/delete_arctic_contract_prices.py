"""
This script will delete (some or all) contract prices for given instrument
"""
from sysobjects.contract_dates_and_expiries import contractDate
from sysobjects.contracts import futuresContract
from sysdata.arctic.arctic_futures_per_contract_prices import arcticFuturesContractPriceData
from sysobjects.instruments import futuresInstrument
from sysproduction.data.prices import get_valid_instrument_code_from_user

def process_contract_date_list( all_contract_dates:list, input:str ):
    selected_dates = []
    if input == 'ALL':
        for date_str in all_contract_dates:
            selected_dates.append(contractDate(date_str))
    else:
        dates = input.split(' ')
        for date_entry in dates:
            try:
                if date_entry[:2] == '<=':
                    comparison_date = contractDate(date_entry[2:]).first_contract_date.as_date()
                    for date_str in all_contract_dates:
                        cd = contractDate(date_str)
                        if cd.first_contract_date.as_date() <= comparison_date:
                            selected_dates.append(cd)
                else:
                    selected_dates.append( contractDate(date_entry) )
            except:
                print("Invalid contract date %s" % date_entry)

    return selected_dates



def delete_contract_prices_for_instrument(instrument_code:str):
    prices = arcticFuturesContractPriceData()
    contracts = prices.contracts_with_price_data_for_instrument_code(instrument_code)
    contract_dates = contracts.contract_date_str_for_contracts_in_list_for_instrument_code(instrument_code)
    contract_dates.sort()
    print("Available contracts: ", contract_dates)

    print("Enter contract date(s) or ALL if you want to delete all instrument contracts.")
    print("Contract dates can be given as a single contract, a list of contracts (separated by a space)")
    print("or comparison (e.g. '<=19980600'). Currently only '<=' operator supported.")
    input_str = input("Selection: ")

    contract_date_list = process_contract_date_list(contract_dates, input_str)
    print("Please confirm that you want to delete the following contracts for instrument %s:" % instrument_code)
    print(contract_date_list)

    confirmation = input("Enter YES for confirmation: ")
    if confirmation=='YES':
        print("Deleting contract(s)..")
        try:
            for cdate in contract_date_list:
                instrument_object = futuresInstrument(instrument_code)
                fc = futuresContract(instrument_object, cdate )
                prices.delete_prices_for_contract_object(fc, areyousure=True)
        except:
            print("Could not delete contract ", cdate)
    else:
        print("Prices NOT deleted")


if __name__ == "__main__":
    print("WARNING! This is a script for deleting contracts from Arctic database! CTRL-C to abort")
    instrument_code = get_valid_instrument_code_from_user(source='single')
    delete_contract_prices_for_instrument(instrument_code)
