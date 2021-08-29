"""
Base class for getting historical data per contract and dumping into mongodb
Includes option to do manual price checking instead of basic spike checking and reporting

Used by implementation classes for various data sources (Interactive Brokers, Norgate, Parametric CSV database)
"""
from datetime import datetime
from syscore.objects import success, failure, missing_data, arg_not_supplied
from syscore.merge_data import spike_in_data
from sysobjects.contracts import futuresContract
from sysobjects.futures_per_contract_prices import futuresContractPrices
from sysdata.futures.futures_per_contract_prices import futuresContractPriceData
from sysdata.data_blob import dataBlob
from sysproduction.data.prices import diagPrices, updatePrices
from syslogdiag.email_via_db_interface import send_production_mail_msg
from sysdata.futures.manual_price_checker import manual_price_checker
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ, from_frequency_to_config_frequency

ALL_INSTRUMENTS = "ALL"

CONFIG_SCHEDULE = "schedule"
CONFIG_INCLUDE_INSTRUMENTS = "include_instruments"
CONFIG_EXCLUDE_INSTRUMENTS = "exclude_instruments"
CONFIG_FREQUENCY = "frequency"
CONFIG_DAYS = "days"
CONFIG_WEEKDAYS = "weekdays"
CONFIG_WEEKEND = "weekend"
CONFIG_ALL_DAYS = "ALL"
CONFIG_ALL_FREQUENCIES = "ALL"

class updateHistoricalPricesBase(object):
    def __init__(self, data, datasource:str, config):
        self.data = data
        self.config = config
        self.datasource = datasource
        self.manual_price_check = False

    def update_historical_prices(self, instrument_code:str = ALL_INSTRUMENTS, manual_price_check:bool = False):
        self.manual_price_check = manual_price_check
        self.update_historical_prices_with_data(self.data, instrument_code)


    def update_historical_prices_with_data(self, data: dataBlob, instrument_code:str):
        price_data = diagPrices(data)
        list_of_codes_all = price_data.get_list_of_instruments_in_multiple_prices()
        if instrument_code == ALL_INSTRUMENTS:
            for instrument in list_of_codes_all:
                data.log.label(instrument_code = instrument)
                self.update_historical_prices_for_instrument(
                    instrument, data)
        else:
            if instrument_code in list_of_codes_all:
                data.log.label(instrument_code = instrument_code)
                self.update_historical_prices_for_instrument(
                    instrument_code, data)
            else:
                data.log.warn("Instrument %s does not have existing multiple prices or is not available from datasource %s" % (instrument_code, self.datasource))


    def get_data_broker(self) -> futuresContractPriceData:
        raise NotImplementedError("Needs to be implemented in child class")

    def update_historical_prices_for_instrument(self, instrument_code: str, data: dataBlob):
        raise NotImplementedError("Needs to be implemented in child class")


    def get_and_add_prices_for_frequency(
            self, data: dataBlob, contract_object: futuresContract, frequency: Frequency = DAILY_PRICE_FREQ):
        db_futures_prices = updatePrices(data)
        price_data = diagPrices(data)

        if not self.is_update_allowed(contract_object.instrument_code, frequency):
            return

        data_broker = self.get_data_broker()

        broker_prices = data_broker.get_prices_at_frequency_for_contract_object(
            contract_object, frequency)

        if len(broker_prices)==0:
            data.log.msg("No prices from broker for %s" % str(contract_object))
            return failure

        if self.manual_price_check == True:
            # -------- Update while doing manual price check  -----------
            old_prices = price_data.get_prices_for_contract_object(contract_object)
            print(
                "\n\n Manually checking prices for %s \n\n" %
                str(contract_object))
            new_prices_checked = manual_price_checker(
                old_prices,
                broker_prices,
                column_to_check="FINAL",
                delta_columns=["OPEN", "HIGH", "LOW"],
                type_new_data=futuresContractPrices,
            )
            db_futures_prices.update_prices_for_contract(
                contract_object, new_prices_checked, check_for_spike=False
            )
        else:
            # -------- Update while checking and reporting for spikes  ----------
            error_or_rows_added = db_futures_prices.update_prices_for_contract(
                contract_object, broker_prices, check_for_spike=True
            )

            if error_or_rows_added is spike_in_data:
                self.report_price_spike(data, contract_object)
                return failure

            data.log.msg(
                "Added %d rows at frequency %s for %s"
                % (error_or_rows_added, frequency, str(contract_object))
            )
        return success


    def report_price_spike(self, data: dataBlob, contract_object: futuresContract):
        # SPIKE
        # Need to email user about this as will need manually checking
        msg = (
                "Spike found in prices for %s: need to manually check by running interactive_manual_check_historical_prices" %
                str(contract_object))
        data.log.warn(msg)
        try:
            send_production_mail_msg(
                data, msg, "Price Spike %s" %
                        contract_object.instrument_code)
        except BaseException:
            data.log.warn(
                "Couldn't send email about price spike for %s"
                % str(contract_object)
            )


    def is_update_allowed(self, instrument_code: str, frequency: Frequency):
        """
        Check if daily or intraday price updates are scheduled 
        for given instrument and frequency
        :param instrument_code: str
        :param frequency: Frequency 
        :return: True if update allowed
        """
        if self.config is not arg_not_supplied:
            if CONFIG_SCHEDULE in self.config:
                # go through each schedule entry and check if the instrument is included
                # AND not excluded, AND frequency is a match AND today is list of scheduled update days
                for schedule_name in self.config[CONFIG_SCHEDULE]:
                    schedule = self.config[CONFIG_SCHEDULE][schedule_name]
                    if not is_instrument_and_frequency_allowed( instrument_code, frequency, schedule):
                        # either instrument or frequency wasn't allowed on this schedule
                        continue

                    if CONFIG_DAYS in schedule:
                        config_days = schedule[CONFIG_DAYS]
                        days = None
                        if config_days == CONFIG_WEEKDAYS:
                            days = [0,1,2,3,4]  # Monday-Friday
                        if config_days == CONFIG_WEEKEND:
                            days = [5,6]  # Saturday, Sunday
                        if config_days == CONFIG_ALL_DAYS:
                            days = [0,1,2,3,4,5,6]
                        if days == None:
                            if isinstance(config_days, list):
                                days = config_days
                            else:
                                days = [config_days] # a single numeric day. Let's convert to list

                        if datetime.now().weekday() not in days:
                            # today is not on scheduled day list
                            continue

                    else:
                        raise Exception("Schedule (%s:%s) does not have days configured!" %
                            (self.datasource, schedule_name))

                    # all criteria passed for this schedule
                    return True

                # by default NOT allowed if none of the schedules matched
                return False
            else:
                # no schedules defined. Let's check if more general include/exlude_instruments and frequency settings are configured
                # for this data source in 'config' section. If there's no such configuration, it's a pass anyway then
                return is_instrument_and_frequency_allowed(instrument_code, frequency, self.config)
        else:
            # allowed by default if config is missing
            return True



def is_instrument_and_frequency_allowed( instrument_code, frequency, config_entry ):

    if CONFIG_INCLUDE_INSTRUMENTS in config_entry:
        if not does_item_match_config_entry( instrument_code, 
            config_entry[CONFIG_INCLUDE_INSTRUMENTS], ALL_INSTRUMENTS ):
            # this instrument wasn't on the included list
            return False
    else:
        # if list of included instruments is not given, by default
        # all instruments are included
        pass
    if CONFIG_EXCLUDE_INSTRUMENTS in config_entry:
        if does_item_match_config_entry( instrument_code, 
            config_entry[CONFIG_EXCLUDE_INSTRUMENTS], ALL_INSTRUMENTS ):
            # this instrument is on the excluded list 
            return False
    if CONFIG_FREQUENCY in config_entry:
        config_freq_str = from_frequency_to_config_frequency(frequency)
        if not does_item_match_config_entry( config_freq_str, 
            config_entry[CONFIG_FREQUENCY], CONFIG_ALL_FREQUENCIES ):
            # this frequency wasn't configured on this schedule entry
            return False
    else:
        # by default all frequencies are included if the 
        # list of frequencies is not given
        pass

    # all criteria passed
    return True


def does_item_match_config_entry(item, config_entry, match_all_item ):
    """
    Config entry can be an item or a list of items. This checks if
    the item matches the config entry OR the config entry is a list 
    containing the item OR config_entry is a 'match all item'
    """
    if config_entry == match_all_item:
        return True
    if item == config_entry:
        return True
    if isinstance(config_entry, list):
        if item in config_entry:
            return True
    return False

