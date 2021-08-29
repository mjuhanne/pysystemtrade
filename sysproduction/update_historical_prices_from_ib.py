"""
Update historical data per contract from interactive brokers data, dump into mongodb
"""
from syscore.objects import success, failure
from syscore.objects import arg_not_supplied
from syscore.dateutils import DAILY_PRICE_FREQ, Frequency
from sysobjects.contracts import futuresContract
from sysdata.data_blob import dataBlob
from sysproduction.data.prices import diagPrices
from sysproduction.data.contracts import dataContracts
from sysbrokers.IB.ib_futures_contract_price_data import ibFuturesContractPriceData, futuresContract
from sysdata.futures.futures_per_contract_prices import futuresContractPriceData
from sysproduction.update_historical_prices_base import updateHistoricalPricesBase, ALL_INSTRUMENTS


def update_historical_prices(data:dataBlob = arg_not_supplied, datasource:str = "IB", instrument_code:str = ALL_INSTRUMENTS, 
    manual_price_check:bool = False, config = arg_not_supplied):
    """
    Do a daily update for futures contract prices, using historical data from IB
    :param data dataBlob
    :param datasource:str Name of this datasource
    :param instrument_code:str Instrument for which prices are to be updated or 'ALL'
    :param manual_price_check:bool If true, instead of reporting price spikes we run manual price checking 
    :param config Yaml configuration entry that has optional schedule settings
    :return: Nothing
    """
    if data is arg_not_supplied:
        data = dataBlob(log_name="Update-Historical-Prices-%s" % datasource )

    update_historical_price_object = updateHistoricalPricesIB(data=data, datasource=datasource, config=config )
    update_historical_price_object.update_historical_prices(instrument_code, manual_price_check=manual_price_check)
    return success


class updateHistoricalPricesIB(updateHistoricalPricesBase):
    def __init__(self, data, datasource:str = "IB", config = arg_not_supplied):
        super().__init__(data, datasource, config)
        self.config = config
        data.add_class_list([ibFuturesContractPriceData])

    def get_data_broker(self) -> futuresContractPriceData:
        return self.data.broker_futures_contract_price


    def update_historical_prices_for_instrument(self, instrument_code: str, data: dataBlob):
        """
        Do a daily update for futures contract prices, using IB historical data

        :param instrument_code: str
        :param data: dataBlob
        :return: None
        """
        diag_contracts = dataContracts(data)
        all_contracts_list = diag_contracts.get_all_contract_objects_for_instrument_code(
            instrument_code)
        contract_list = all_contracts_list.currently_sampling()

        if len(contract_list) == 0:
            data.log.warn("No contracts marked for sampling for %s" % instrument_code)
            return failure

        for contract_object in contract_list:
            data.log.label(contract_date = contract_object.date_str)
            self.update_historical_prices_for_instrument_and_contract(
                contract_object, data)

        return success


    def update_historical_prices_for_instrument_and_contract(
            self, contract_object: futuresContract, data: dataBlob):
        """
        Do a daily update for futures contract prices, using IB historical data

        There are two different calls, the first using intraday frequency, the second daily.
        So in a typical session we'd get the hourly intraday prices for a given instrument,
        and then if that instrument has closed we'd also get a new end of day price.
        The end of day prices are given the artifical datetime stamp of 23:00:00
        (because I never collect data round that time).

        If the intraday call fails, we don't want to get daily data.
        Otherwise we wouldn't be able to subsequently backfill hourly prices that occured
        before the daily close being added (the code doesn't allow you to add prices that have
        a timestamp which is before the last timestamp in the existing data).

        That's why the result of the first call matters, and is used to abort the function prematurely before we get to daily data.
        We don't care about the second call succeeding or failing, and so the result of that is ignored.

        :param contract_object: futuresContract
        :param data: data blob
        :return: None
        """
        diag_prices = diagPrices(data)
        intraday_frequency = diag_prices.get_intraday_frequency_for_historical_download()
        daily_frequency = DAILY_PRICE_FREQ

        # Get *intraday* data (defaults to hourly)
        result = self.get_and_add_prices_for_frequency(
            data, contract_object, frequency=intraday_frequency
        )
        if result is failure:
            # Skip daily data if intraday not working
            return None

        # Get daily data
        # we don't care about the result flag for this
        self.get_and_add_prices_for_frequency(
            data, contract_object, frequency=daily_frequency)



if __name__ == "__main__":
    print("Update price data from IB")

    data = dataBlob(log_name="update_historical_prices")

    price_data = diagPrices(data)
    instruments = price_data.get_list_of_instruments_in_multiple_prices()
    print("Available instruments containing multiple prices: ", instruments)

    print("Enter instrument code (or ALL)")
    instrument_code = input("Selection: <return to abort> ")
    if instrument_code == "":
        exit()

    obj = updateHistoricalPricesIB(data, "IB")

    if instrument_code == 'ALL':
        obj.update_historical_prices()
    else:
        obj.update_historical_prices_for_instrument(instrument_code, data)
