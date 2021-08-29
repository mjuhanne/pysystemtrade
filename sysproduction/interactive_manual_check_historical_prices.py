"""
Update historical data per contract from relevant data sources with manual checking and dump into mongodb

Apply a check to each price series
"""

from syscore.objects import success
from sysdata.data_blob import dataBlob
from sysproduction.data.prices import (
    diagPrices,
    get_valid_instrument_code_from_user,
)
from sysproduction.update_historical_prices import updateHistoricalPrices



def interactive_manual_check_historical_prices():
    """
    Do a daily update for futures contract prices, using historical 
    data from various configured sources

    If any 'spikes' are found, run manual checks

    :return: Nothing
    """
    with dataBlob(log_name="Update-Historical-prices-manually") as data:
        check_historical_price_object = checkHistoricalPrices(data)
        check_historical_price_object.check_and_update_historical_prices()


class checkHistoricalPrices(object):
    def __init__(self, data):
        self.data = data

    def check_and_update_historical_prices_from_all_sources(self, instrument_code:str):
        update_historical_price_object = updateHistoricalPrices(self.data)
        update_historical_price_object.update_historical_prices(instrument=instrument_code, manual_price_check=True)

    def check_and_update_historical_prices(self):
        data = self.data
        do_another = True
        while do_another:
            EXIT_STR = "Finished: Exit"
            instrument_code = get_valid_instrument_code_from_user(
                data, source="single", allow_exit=True, exit_code=EXIT_STR
            )
            if instrument_code is EXIT_STR:
                do_another = False
            else:
                self.check_instrument_ok_for_broker(data, instrument_code)
                data.log.label(instrument_code=instrument_code)
                self.check_and_update_historical_prices_from_all_sources(instrument_code)

        return success

    def check_instrument_ok_for_broker(self, data: dataBlob, instrument_code: str):
        diag_prices = diagPrices(data)
        list_of_codes_all = diag_prices.get_list_of_instruments_with_contract_prices()
        if instrument_code not in list_of_codes_all:
            print(
                "\n\n\ %s is not an instrument with price data \n\n" %
                instrument_code)
            raise Exception()


if __name__ == "__main__":
    interactive_manual_check_historical_prices()
