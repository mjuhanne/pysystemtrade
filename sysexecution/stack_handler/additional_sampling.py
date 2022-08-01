from syscore.dateutils import Frequency
from sysdata.config.configdata import Config
from sysexecution.stack_handler.stackHandlerCore import stackHandlerCore
from sysobjects.contracts import futuresContract
from syscore.objects import missing_data, no_market_permissions, failure
from sysdata.futures.virtual_futures_data import virtualFuturesData


class stackHandlerAdditionalSampling(stackHandlerCore):
    def refresh_additional_sampling_all_instruments(self):
        all_contracts = self.get_all_instruments_priced_contracts()
        for contract in all_contracts:
            self.refresh_sampling_for_contract(contract)

    def get_all_instruments_priced_contracts(self):
        ## Cache for speed
        priced_contracts = getattr(self, "_all_priced_contracts", None)
        if priced_contracts is None:
            priced_contracts = self._get_all_instruments_priced_contracts_from_db()
            self._all_priced_contracts = priced_contracts

        return priced_contracts

    def _get_all_instruments_priced_contracts_from_db(self):
        instrument_list = self._get_all_instruments()
        data_contracts = self.data_contracts

        priced_contracts = [
            futuresContract(
                instrument_code, data_contracts.get_priced_contract_id(instrument_code)
            )
            for instrument_code in instrument_list
        ]

        return priced_contracts

    def _get_all_instruments(self):
        diag_prices = self.diag_prices
        instrument_list = diag_prices.get_list_of_instruments_in_multiple_prices()

        return instrument_list

    def refresh_sampling_for_contract(self, contract: futuresContract):

        okay_to_sample = self.is_contract_currently_okay_to_sample(contract)
        if not okay_to_sample:
            return None

        self.refresh_sampling_without_checks(contract)

    def is_sampling_omitted_for_instrument(self, instrument_code:str):
        omit_list = self.config.get_element_or_missing_data("omit_sampling_for_instruments")
        if omit_list is not missing_data:
            for omit_instrument in omit_list:
                # Not very versatile wildcard functionality
                if omit_instrument[-1] == '*':
                    partial_instrument_code = omit_instrument[:-1]
                    if partial_instrument_code == instrument_code[:len(partial_instrument_code)]:
                        return True
                else:
                    if instrument_code == omit_instrument:
                        return True
        return False

    def is_contract_currently_okay_to_sample(self, contract: futuresContract) -> bool:
        if self.is_sampling_omitted_for_instrument(contract.instrument_code):
            return False
        data_broker = self.data_broker
        okay_to_sample = data_broker.is_contract_okay_to_trade(contract)
        return okay_to_sample

    def refresh_sampling_without_checks(self, contract: futuresContract):
        intraday_prices = self.get_intraday_prices(contract)
        if intraday_prices is not missing_data:
            self.add_intraday_prices_to_db(contract, intraday_prices)

        if not virtualFuturesData.is_virtual(contract.instrument_code):
            carry_contract_date = self.data_contracts._get_carry_contract_id(contract.instrument_code)
            carry_contract = futuresContract.from_two_strings(contract.instrument_code, carry_contract_date)
            intraday_prices = self.get_intraday_prices(carry_contract)
            if intraday_prices is not missing_data:
                self.add_intraday_prices_to_db(carry_contract, intraday_prices)

        average_spread = self.get_average_spread(contract)
        if average_spread is not missing_data:
            self.add_spread_data_to_db(contract, average_spread)

    def get_intraday_prices(self, contract: futuresContract):
        data_broker = self.data_broker
        intraday_prices = data_broker.get_prices_at_frequency_for_contract_object(contract, Frequency.Hour)
        if intraday_prices is no_market_permissions or intraday_prices is failure:
            return missing_data
        return intraday_prices

    def add_intraday_prices_to_db(self, contract: futuresContract, intraday_prices):
        update_prices = self.update_prices
        update_prices.add_intraday_prices(contract, intraday_prices)


    def get_average_spread(self, contract: futuresContract) -> float:
        data_broker = self.data_broker
        tick_data = data_broker.get_recent_bid_ask_tick_data_for_contract_object(
            contract
        )
        if tick_data is no_market_permissions or tick_data is missing_data:
            return missing_data

        average_spread = tick_data.average_bid_offer_spread(remove_negative=True)

        ## Shouldn't happen, but just in case
        if average_spread is not missing_data:
            if average_spread < 0.0:
                return missing_data

        return average_spread

    def add_spread_data_to_db(self, contract: futuresContract, average_spread: float):

        ## we store by instrument
        instrument_code = contract.instrument_code
        update_prices = self.update_prices

        update_prices.add_spread_entry(instrument_code, spread=average_spread)
