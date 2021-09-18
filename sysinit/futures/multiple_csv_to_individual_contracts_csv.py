"""
This script will get multiple prices for all the instruments found in
the provided csv repository (/data/futures/multiple_prices_csv/)
and separate them into individual contracts. These will be then saved
into target directory. Each instrument will additionally be separated into
its own subdirectory.

The price data will be analyzed further in relation to the expected roll date and 
expiration date and warnings will be created if missing data is suspected.
Additionally csv_convert.log file will be created that you can analyze later

Note that to get complete data you DO need to download some of the data from other
sources (such as Barchart). For example in multiple price series the CORN and SOYBEAN do 
not have carry contracts available regularly. Also CRUDE_W_mini and KOSPI_mini are incomplete
because the historical data is based on the non-mini contracts but the roll 
cycles for the mini contracts have been updated. What's also missing from the
multiple series is volume data which are then substituted simply with a bogus volume of 1
"""

from syscore.objects import arg_not_supplied
from sysdata.csv.csv_multiple_prices import csvFuturesMultiplePricesData
from sysdata.csv.csv_futures_contract_prices import csvFuturesContractPriceData
import pandas as pd
import datetime
import os

from sysdata.mongodb.mongo_roll_data import mongoRollParametersData
from sysobjects.contract_dates_and_expiries import contractDate, expiryDate
from sysobjects.rolls import contractDateWithRollParameters

from sysobjects.contracts import futuresContract
from sysobjects.futures_per_contract_prices import futuresContractPrices

missing_data = dict()
newest_complete_contract = dict()

class MissingData: 
    def __init__(self, _contract_date, _exp_date, _roll_date, _last_entry, _missing_days):
        self.exp_date = _exp_date
        self.roll_date = _roll_date
        self.contract_date = _contract_date
        self.last_entry = _last_entry
        self.missing_days = _missing_days


def from_multiple_to_individual_contract_prices_csv(multiple_price_datapath=arg_not_supplied, individual_price_datapath=arg_not_supplied ):
    csv_multiple_prices = csvFuturesMultiplePricesData(multiple_price_datapath)

    instrument_codes = csv_multiple_prices.get_list_of_instruments()
    for instrument_code in instrument_codes:
        from_multiple_to_individual_contract_prices_csv_by_instrument(instrument_code, multiple_price_datapath = multiple_price_datapath,
                                            individual_price_datapath = individual_price_datapath)



def from_multiple_to_individual_contract_prices_csv_by_instrument(instrument_code:str, multiple_price_datapath =arg_not_supplied,
                                                individual_price_datapath = arg_not_supplied):
    print("Processing instrument: ", instrument_code)

    csv_mult_data = csvFuturesMultiplePricesData(multiple_price_datapath)

    datapath = individual_price_datapath + "/" + instrument_code
    try: 
        os.makedirs(datapath)
    except FileExistsError:
        print(datapath, " already exists")

    csv_individual_data = csvFuturesContractPriceData(datapath)

    missing_data[instrument_code] = []

    mult_prices = csv_mult_data.get_multiple_prices(instrument_code)
    mdict = mult_prices.as_dict()
    mult_columns = { 'PRICE':'PRICE_CONTRACT', 'CARRY':'CARRY_CONTRACT', 'FORWARD':'FORWARD_CONTRACT' }

    # get roll parameters to calculate approximate roll date in order to make sure we have 
    # complete price data for each contract available 
    rollparameters = mongoRollParametersData()
    roll_parameters_object = rollparameters.get_roll_parameters(instrument_code)

    # Get all unique contract dates from multiple price series
    contracts = pd.Series()
    for key,val in mult_columns.items():
        contracts = contracts.append( mdict[key][val] )
    contracts = contracts.unique()
    contracts.sort()

    print(instrument_code, " contracts: ", contracts)
    for contract_date in contracts:
        print("Processing ", instrument_code, " : ", contract_date)

        contract_prices = futuresContractPrices.create_empty()
        contract_prices_multiple = dict()

        for key, val in mult_columns.items():
            # extract each contract price data separately from each PRICE/CARRY/FORWARD column

            # query e.g. PRICE_CONTRACT == '20210600' & PRICE == PRICE
            # The last comparison will ensure that NaN values will not be included
            q = val + " == '" + contract_date + "' & " + key + " == " + key
            cprice = mdict[key].query(q)

            if cprice.size > 0:
                fcp_df = pd.DataFrame()
                # WARNING! OPEN/HIGH/LOW values are set to be FINAL(CLOSE) values. Also bogus
                # volume data is created since it isn't available in multiple price series!
                fcp_df["OPEN"] = cprice[key]
                fcp_df["HIGH"] = cprice[key]
                fcp_df["LOW"] = cprice[key]
                fcp_df["FINAL"] = cprice[key]
                fcp_df["VOLUME"] = 1
                fcp = futuresContractPrices(fcp_df)
            else:
                # just a filler
                fcp = futuresContractPrices.create_empty()

            #print(fcp)
            contract_prices_multiple[key] = fcp

        # .. then we combine the PRICE/CARRY/FORWARD prices to a singular contract dataset
        for key, val in mult_columns.items():
            contract_prices = contract_prices.merge_with_other_prices( contract_prices_multiple[key], only_add_rows=False, check_for_spike=False) 

        contract = futuresContract.from_two_strings( instrument_code, contract_date)

        # for some contracts there's price data missing few days before the approximate roll date 
        # Allow this because this will be eventually handled when adjusting the roll calendars
        expiry_allowance = datetime.timedelta( days=8 )

        if contract.contract_date.only_has_month:
            contract.contract_date.update_expiry_date_with_new_offset( roll_parameters_object.approx_expiry_offset)
        roll_date = contractDateWithRollParameters( contract.contract_date, roll_parameters_object).desired_roll_date

        if contract_prices.size == 0:
            print("Warning! No price data for contract ", contract_date)
            missing_data[instrument_code].append(MissingData(contract_date, contract.contract_date.expiry_date, roll_date, "NA", "NA"))
        else:
            last_entry = contract_prices.index.sort_values()[-1]

            #print(contract_prices)

            if roll_parameters_object.hold_rollcycle.check_is_month_in_rollcycle( contract.contract_date.letter_month() ):
                # For hold contracts we need to check if we have enough data

                print("expiry: ", contract.contract_date.expiry_date, " last_entry: ", last_entry, "roll_date: ", roll_date)

                if (last_entry < roll_date - expiry_allowance):
                    missing_days = roll_date - last_entry
                    print("Warning! Not enough data is available until appproximate roll date! Missing days: ", missing_days, " (This is ok if this is one of the newest contracts)")
                    missing_data[instrument_code].append( MissingData( contract_date,
                        contract.contract_date.expiry_date, roll_date, last_entry, missing_days.days))
                else:
                    newest_complete_contract[instrument_code] = contract_date

            csv_individual_data.write_prices_for_contract_object(contract, contract_prices)



if __name__ == "__main__":
    ## modify target datapath
    individual_price_datapath = "***CSV DATAPATH REQUIRED***"

    # need not be specified if using provided csv files
    multiple_price_datapath = arg_not_supplied

    from_multiple_to_individual_contract_prices_csv(multiple_price_datapath=multiple_price_datapath, individual_price_datapath=individual_price_datapath)

    f = open(individual_price_datapath + "/csv_convert.log","w")

    if len(missing_data) > 0:
        print("Historical price data is incomplete for following contracts:", file=f)
        print("Historical price data is incomplete for following contracts:")
        for key, val in missing_data.items():
            for md in val:
                print(key, " : ", md.contract_date, "(approx. expiry/roll dates: ", md.exp_date.strftime("%Y-%m-%d"), "/", md.roll_date.strftime("%Y-%m-%d"), ", last entry: ", md.last_entry, " -> missing days: ", md.missing_days, ")")
                print(key, " : ", md.contract_date, "(approx. expiry/roll dates: ", md.exp_date.strftime("%Y-%m-%d"), "/", md.roll_date.strftime("%Y-%m-%d"), ", last entry: ", md.last_entry, " -> missing days: ", md.missing_days, ")", file=f)

    print("\r\nLast complete contract by instrument:")
    print("\r\nLast complete contract by instrument:", file=f)
    for instrument_code in newest_complete_contract:
        print(instrument_code," : ", newest_complete_contract[instrument_code])
        print(instrument_code," : ", newest_complete_contract[instrument_code], file=f)
    f.close()
