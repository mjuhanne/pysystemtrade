"""
Update historical data per contract from Norgate, dump into mongodb
"""
from sysdata.data_blob import dataBlob
from syscore.objects import success, failure, arg_not_supplied
from sysproduction.data.prices import diagPrices
from syscore.dateutils import DAILY_PRICE_FREQ, Frequency
from syscore.objects import missing_instrument
from sysdata.norgate.norgate_futures_per_contract_prices import norgateFuturesContractPriceData, futuresContract
from sysproduction.update_historical_prices_base import updateHistoricalPricesBase, ALL_INSTRUMENTS
from sysdata.futures.futures_per_contract_prices import futuresContractPriceData


def update_historical_prices(data:dataBlob = arg_not_supplied, datasource:str = "Norgate", instrument_code:str = ALL_INSTRUMENTS, 
    manual_price_check:bool = False, config = arg_not_supplied ):
    """
    Do a daily update for futures contract prices, using Norgate historical data
    :param data dataBlob
    :param datasource:str Name of this datasource
    :param instrument_code:str Instrument for which prices are to be updated or 'ALL'
    :param manual_price_check:bool If true, instead of reporting price spikes we run manual price checking 
    :param config Yaml configuration entry that has optional schedule settings
    :return: Nothing
    """
    if data is arg_not_supplied:
        data = dataBlob(log_name="Update-Historical-Prices-%s" % datasource )

    update_historical_price_object = updateHistoricalPricesNorgate(data=data, datasource=datasource, config=config)
    update_historical_price_object.update_historical_prices(instrument_code, manual_price_check)
    return success


class updateHistoricalPricesNorgate(updateHistoricalPricesBase):
    def __init__(self, data, datasource:str = "Norgate", config = arg_not_supplied):
        super().__init__(data, datasource, config)
        data.add_class_list([norgateFuturesContractPriceData])

    def get_data_broker(self) -> futuresContractPriceData:
        return self.data.broker_futures_contract_price


    def update_historical_prices_for_instrument(self, instrument_code: str, data: dataBlob):
        """
        Do a daily update for futures contract prices, using Norgate historical data

        :param instrument_code: str
        :param data: dataBlob
        :return: None
        """
        data_broker = self.get_data_broker()
        contract_list = data_broker.contracts_with_price_data_for_instrument_code(instrument_code,
            allow_expired=False)
        if contract_list is missing_instrument:
            print("Prices for instrument", instrument_code, " is not provided by Norgate")
            return failure

        for contract_object in contract_list:
            data.log.label(contract_date = contract_object.date_str)
            self.update_historical_prices_for_instrument_and_contract(
                contract_object, data)

        return success


    def update_historical_prices_for_instrument_and_contract(
            self, contract_object: futuresContract, data: dataBlob):
        """
        Do a daily update for futures contract prices, using Norgate historical data

        :param contract_object: futuresContract
        :param data: data blob
        :return: None
        """

        # Get only daily data
        result = self.get_and_add_prices_for_frequency(
            data, contract_object, frequency=DAILY_PRICE_FREQ)
        
        return result


if __name__ == "__main__":
    print("Update price data from Norgate")

    data_historical = dataBlob(log_name="Update-Historical-Prices-Norgate")
    obj = updateHistoricalPricesNorgate(data_historical)

    instruments = data_historical.broker_futures_contract_price.get_list_of_instrument_codes_with_price_data()
    instruments.sort()
    print("Available instruments from Norgate: ", instruments)

    price_data = diagPrices(data_historical)
    mult_instruments = price_data.get_list_of_instruments_in_multiple_prices()
    print("\nInstruments containing multiple prices: ", mult_instruments)

    print("\nEnter instrument code. ")
    print("Alternatively enter 'ALL' for updating prices for all instruments or")
    print("'MULT' for updating only those instruments containing multiple prices.")
    instrument_code = input("Selection: <return to abort> ")
    if instrument_code == "":
        exit()

    obj = updateHistoricalPricesNorgate(data_historical)

    if instrument_code == "ALL":
        for instr in instruments:
            obj.update_historical_prices_for_instrument(instr, data_historical)
    else:
        if instrument_code == 'MULT':
            obj.update_historical_prices()
        else:
            obj.update_historical_prices_for_instrument(instrument_code, data_historical)
