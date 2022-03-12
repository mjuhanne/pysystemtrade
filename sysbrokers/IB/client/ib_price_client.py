from dateutil.tz import tz
from datetime import timedelta

import datetime
import pandas as pd
import numpy as np

from ib_insync import Contract as ibContract, Ticker
from ib_insync import util

from sysbrokers.IB.client.ib_client import (
    IB_ERROR_TYPES, 
    PACING_INTERVAL_SECONDS, 
    IB_ERROR__NO_MARKET_PERMISSIONS,
    IB_ERROR__NO_HEAD_TIME_STAMP,
)
from sysbrokers.IB.client.ib_contracts_client import ibContractsClient
from sysbrokers.IB.ib_positions import resolveBS_for_list

from syscore.objects import missing_contract, missing_data, no_market_permissions
from syscore.dateutils import (
    adjust_timestamp_to_include_notional_close_and_time_offset,
    strip_timezone_fromdatetime,
    Frequency,
    DAILY_PRICE_FREQ,
)

from syslogdiag.logger import logger
from syslogdiag.log_to_screen import logtoscreen

from sysobjects.contracts import futuresContract
from sysexecution.trade_qty import tradeQuantity

IB_MARKET_DATE_TYPE__LIVE = 1
IB_MARKET_DATE_TYPE__FROZEN = 2
IB_MARKET_DATE_TYPE__DELAYED = 3
IB_MARKET_DATE_TYPE__FROZEN_DELAYED = 4
TICKER_TIMEOUT = timedelta(seconds=10)

class tickerWithBS(object):
    def __init__(self, ticker, BorS: str):
        self.ticker = ticker
        self.BorS = BorS


# we don't include ibClient since we get that through contracts client
class ibPriceClient(ibContractsClient):
    def broker_get_historical_futures_data_for_contract(
        self,
        contract_object_with_ib_broker_config: futuresContract,
        bar_freq: Frequency = DAILY_PRICE_FREQ,
        whatToShow="TRADES",
        startDateTime="",
        allow_expired=False,
    ) -> pd.DataFrame:
        """
        Get historical daily data

        :param contract_object_with_ib_broker_config: contract where instrument has ib metadata
        :param freq: str; one of D, H, 5M, M, 10S, S
        :return: futuresContractPriceData
        """

        specific_log = contract_object_with_ib_broker_config.specific_log(self.log)

        ibcontract = self.ib_futures_contract(
            contract_object_with_ib_broker_config, allow_expired=allow_expired
        )

        if ibcontract is missing_contract:
            specific_log.warn(
                "Can't resolve IB contract %s"
                % str(contract_object_with_ib_broker_config)
            )
            return missing_data

        price_data = self._get_generic_data_for_contract(
            ibcontract, log=specific_log, bar_freq=bar_freq, whatToShow=whatToShow, startDateTime=startDateTime
        )

        return price_data

    def set_market_data_type(self, market_data_type):
        self.ib.reqMarketDataType(market_data_type)

    def get_ticker_object(
        self,
        contract_object_with_ib_data: futuresContract,
        trade_list_for_multiple_legs: tradeQuantity = None,
    ) -> tickerWithBS:

        specific_log = contract_object_with_ib_data.specific_log(self.log)

        ibcontract = self.ib_futures_contract(
            contract_object_with_ib_data,
            trade_list_for_multiple_legs=trade_list_for_multiple_legs,
        )

        if ibcontract is missing_contract:
            specific_log.warn(
                "Can't find matching IB contract for %s"
                % str(contract_object_with_ib_data)
            )
            return missing_contract

        self.ib.reqMktData(ibcontract, "", False, False)
        ticker = self.ib.ticker(ibcontract)
        if self.get_last_error(ibcontract) == IB_ERROR__NO_MARKET_PERMISSIONS:
            return no_market_permissions

        ib_BS_str, ib_qty = resolveBS_for_list(trade_list_for_multiple_legs)

        ticker_with_bs = tickerWithBS(ticker, ib_BS_str)

        return ticker_with_bs

    def cancel_market_data_for_contract_object(
        self,
        contract_object_with_ib_data: futuresContract,
        trade_list_for_multiple_legs: tradeQuantity = None,
    ):

        specific_log = contract_object_with_ib_data.specific_log(self.log)

        ibcontract = self.ib_futures_contract(
            contract_object_with_ib_data,
            trade_list_for_multiple_legs=trade_list_for_multiple_legs,
        )

        if ibcontract is missing_contract:
            specific_log.warn(
                "Can't find matching IB contract for %s"
                % str(contract_object_with_ib_data)
            )
            return missing_contract

        self.ib.cancelMktData(ibcontract)

    def ib_get_recent_bid_ask_tick_data(
        self,
        contract_object_with_ib_data: futuresContract,
        tick_count=200,
    ) -> list:
        """

        :param contract_object_with_ib_data:
        :return:
        """
        specific_log = self.log.setup(
            instrument_code=contract_object_with_ib_data.instrument_code,
            contract_date=contract_object_with_ib_data.date_str,
        )
        if contract_object_with_ib_data.is_spread_contract():
            error_msg = "Can't get historical data for combo"
            specific_log.critical(error_msg)
            raise Exception(error_msg)

        ibcontract = self.ib_futures_contract(contract_object_with_ib_data)

        if ibcontract is missing_contract:
            specific_log.warn(
                "Can't find matching IB contract for %s"
                % str(contract_object_with_ib_data)
            )
            return missing_contract

        recent_ib_time = self.ib.reqCurrentTime() - datetime.timedelta(seconds=60)

        tick_data = self.ib.reqHistoricalTicks(
            ibcontract, recent_ib_time, "", tick_count, "BID_ASK", useRth=False
        )
        if len(tick_data) == 0:
            if self.get_last_error(ibcontract) == IB_ERROR__NO_MARKET_PERMISSIONS:
                return no_market_permissions

        return tick_data

    def is_ticker_complete(self, ticker:Ticker):
        wanted_attribute_list = ['bid','ask','open','high','low','close','volume']
        for wanted_attribute in wanted_attribute_list:
            value = getattr(ticker,wanted_attribute)
            if np.isnan(value):
                return False
        return True

    def wait_until_ticker_is_complete(self, ticker:Ticker, timeout):
        start_time = datetime.datetime.now()
        while datetime.datetime.now() - start_time < timeout:
            self.ib.sleep(1)
            if self.is_ticker_complete(ticker):
                return True
        return False


    def ib_get_delayed_bid_ask_tick_data(
        self,
        contract_object_with_ib_data: futuresContract,
        tick_count=200,
    ) -> Ticker:
        """

        :param contract_object_with_ib_data:
        :return:
        """
        specific_log = self.log.setup(
            instrument_code=contract_object_with_ib_data.instrument_code,
            contract_date=contract_object_with_ib_data.date_str,
        )
        if contract_object_with_ib_data.is_spread_contract():
            error_msg = "Can't get historical data for combo"
            specific_log.critical(error_msg)
            raise Exception(error_msg)

        ibcontract = self.ib_futures_contract(contract_object_with_ib_data)

        if ibcontract is missing_contract:
            specific_log.warn(
                "Can't find matching IB contract for %s"
                % str(contract_object_with_ib_data)
            )
            return missing_contract

        tick_data = self.ib.reqMktData(ibcontract)
        result = self.wait_until_ticker_is_complete(tick_data, TICKER_TIMEOUT)
        self.ib.cancelMktData(ibcontract)
        if not result:
            return missing_data

        return tick_data


    def _get_generic_data_for_contract(
        self,
        ibcontract: ibContract,
        log: logger = None,
        bar_freq: Frequency = DAILY_PRICE_FREQ,
        whatToShow: str = "TRADES",
        startDateTime="",
    ) -> pd.DataFrame:
        """
        Get historical daily data

        :param contract_object_with_ib_data: contract where instrument has ib metadata
        :param freq: str; one of D, H, 5M, M, 10S, S
        :param startDateTime: str/datetime; If "" then then one maximum chunk (1 year for daily frequency) is returned. Otherwise 
            return data from the given date or the earliest available data point
        :return: futuresContractPriceData
        """
        if log is None:
            log = self.log

        try:
            barSizeSetting, barSize, durationStr, duration = _get_barsize_and_duration_from_frequency(
                bar_freq
            )
        except Exception as exception:
            log.warn(exception)
            return missing_data

        if startDateTime=="":
            price_data_raw = self._ib_get_historical_data_of_duration_and_barSize(
                ibcontract,
                durationStr=durationStr,
                barSizeSetting=barSizeSetting,
                whatToShow=whatToShow,
            
                log=log,
            )
        else:
            earliest_data = self.ib.reqHeadTimeStamp(
                ibcontract,
                whatToShow=whatToShow,
                useRTH=True,
            )
            if self.get_last_error(ibcontract) == IB_ERROR__NO_HEAD_TIME_STAMP:
                # let's try 10 years of history
                earliest_data = datetime.datetime.now() - datetime.timedelta(days=365*10)
                if startDateTime < earliest_data:
                    startDateTime = earliest_data
                log.msg(
                    "Couldn't fetch head time stamp! Trying to fetch data starting from %s" 
                    % (
                        startDateTime.strftime("%Y-%m-%d") 
                ) )
            else:
                if startDateTime < earliest_data:
                    startDateTime = earliest_data
                log.msg(
                    "Earliest data: %s, collection starting date: %s" 
                    % (
                        earliest_data.strftime("%Y-%m-%d"), 
                        startDateTime.strftime("%Y-%m-%d") 
                ) )

            price_data_raw = None

            while (startDateTime < datetime.datetime.now()):

                endDateTime = startDateTime + duration
                price_data_chunk = self._ib_get_historical_data_of_duration_and_barSize(
                    ibcontract,
                    durationStr=durationStr,
                    barSizeSetting=barSizeSetting,
                    whatToShow=whatToShow,
                    endDateTime=endDateTime,
                    log=log,
                )
                if price_data_raw is None:
                    price_data_raw = price_data_chunk
                else:
                    price_data_raw = price_data_raw.append(price_data_chunk)

                startDateTime = endDateTime + barSize

        if price_data_raw is None or len(price_data_raw) == 0:
            if self.get_last_error(ibcontract) == IB_ERROR__NO_MARKET_PERMISSIONS:
                return no_market_permissions

            if self.get_last_error(ibcontract) == IB_ERROR__NO_HEAD_TIME_STAMP:
                return missing_data

            # Other error
            raise Exception(
                "Could not fetch %s for contract %s (startTime %s, freq %s)"
                % (whatToShow, str(ibcontract), startDateTime, str(bar_freq))
            )

        price_data_as_df = self._raw_ib_data_to_df(
            price_data_raw=price_data_raw, log=log
        )

        return price_data_as_df

    def _raw_ib_data_to_df(
        self, price_data_raw: pd.DataFrame, log: logger
    ) -> pd.DataFrame:

        if price_data_raw is None:
            log.warn("No price data from IB")
            return missing_data

        price_data_as_df = price_data_raw[["open", "high", "low", "close", "volume"]]

        price_data_as_df.columns = ["OPEN", "HIGH", "LOW", "FINAL", "VOLUME"]

        date_index = [
            self._ib_timestamp_to_datetime(price_row)
            for price_row in price_data_raw["date"]
        ]
        price_data_as_df.index = date_index

        return price_data_as_df

    ### TIMEZONE STUFF
    def _ib_timestamp_to_datetime(self, timestamp_ib) -> datetime.datetime:
        """
        Turns IB timestamp into pd.datetime as plays better with arctic, converts IB time (UTC?) to local,
        and adjusts yyyymm to closing vector

        :param timestamp_str: datetime.datetime
        :return: pd.datetime
        """

        local_timestamp_ib = self._adjust_ib_time_to_local(timestamp_ib)
        timestamp = pd.to_datetime(local_timestamp_ib)

        adjusted_ts = adjust_timestamp_to_include_notional_close_and_time_offset(
            timestamp
        )

        return adjusted_ts

    def _adjust_ib_time_to_local(self, timestamp_ib) -> datetime.datetime:

        if getattr(timestamp_ib, "tz_localize", None) is None:
            # daily, nothing to do
            return timestamp_ib

        # IB timestamp already includes tz
        timestamp_ib_with_tz = timestamp_ib
        local_timestamp_ib_with_tz = timestamp_ib_with_tz.astimezone(tz.tzlocal())
        local_timestamp_ib = strip_timezone_fromdatetime(local_timestamp_ib_with_tz)

        return local_timestamp_ib

    # HISTORICAL DATA
    # Works for FX and futures
    def _ib_get_historical_data_of_duration_and_barSize(
        self,
        ibcontract: ibContract,
        durationStr: str = "1 Y",
        barSizeSetting: str = "1 day",
        whatToShow="TRADES",
        endDateTime="",
        log: logger = None,
    ) -> pd.DataFrame:
        """
        Returns historical prices for a contract, up to today
        ibcontract is a Contract
        :returns list of prices in 4 tuples: Open high low close volume
        """

        if log is None:
            log = self.log

        last_call = self.last_historic_price_calltime
        _avoid_pacing_violation(last_call, log=log)

        bars = self.ib.reqHistoricalData(
            ibcontract,
            endDateTime=endDateTime,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow=whatToShow,
            useRTH=True,
            formatDate=2,
        )
        df = util.df(bars)

        self.last_historic_price_calltime = datetime.datetime.now()

        return df




def _get_barsize_and_duration_from_frequency(bar_freq: Frequency) -> (str, timedelta, str, timedelta):

    barsize_str_lookup = dict(
        [
            (Frequency.Day, "1 day"),
            (Frequency.Hour, "1 hour"),
            (Frequency.Minutes_15, "15 mins"),
            (Frequency.Minutes_5, "5 mins"),
            (Frequency.Minute, "1 min"),
            (Frequency.Seconds_10, "10 secs"),
            (Frequency.Second, "1 secs"),
        ]
    )

    barsize_lookup = dict(
        [
            (Frequency.Day, timedelta(days=1)),
            (Frequency.Hour, timedelta(hours=1)),
            (Frequency.Minutes_15, timedelta(minutes=15)),
            (Frequency.Minutes_5, timedelta(minutes=5)),
            (Frequency.Minute, timedelta(minutes=1)),
            (Frequency.Seconds_10, timedelta(seconds=10)),
            (Frequency.Second, timedelta(seconds=1)),
        ]
    )

    duration_str_lookup = dict(
        [
            (Frequency.Day, "1 Y"),
            (Frequency.Hour, "1 M"),
            (Frequency.Minutes_15, "1 W"),
            (Frequency.Minutes_5, "1 W"),
            (Frequency.Minute, "1 D"),
            (Frequency.Seconds_10, "14400 S"),
            (Frequency.Second, "1800 S"),
        ]
    )

    duration_lookup = dict(
        [
            (Frequency.Day, timedelta(days=365)),
            (Frequency.Hour, timedelta(days=30)),
            (Frequency.Minutes_15, timedelta(days=7)),
            (Frequency.Minutes_5, timedelta(days=7)),
            (Frequency.Minute, timedelta(days=1)),
            (Frequency.Seconds_10, timedelta(hours=4)),
            (Frequency.Second, timedelta(minutes=30)),
        ]
    )

    try:
        assert bar_freq in barsize_str_lookup.keys()
        assert bar_freq in barsize_lookup.keys()
        assert bar_freq in duration_str_lookup.keys()
        assert bar_freq in duration_lookup.keys()
    except:
        raise Exception(
            "Barsize %s not recognised should be one of %s"
            % (str(bar_freq), str(barsize_lookup.keys()))
        )

    ib_barsize_str = barsize_str_lookup[bar_freq]
    ib_barsize = barsize_lookup[bar_freq]
    ib_duration_str = duration_str_lookup[bar_freq]
    ib_duration = duration_lookup[bar_freq]

    return ib_barsize_str, ib_barsize, ib_duration_str, ib_duration


def _avoid_pacing_violation(
    last_call_datetime: datetime.datetime, log: logger = logtoscreen("")
):
    printed_warning_already = False
    while _pause_for_pacing(last_call_datetime):
        if not printed_warning_already:
            log.msg(
                "Pausing %f seconds to avoid pacing violation"
                % (
                    last_call_datetime
                    + datetime.timedelta(seconds=PACING_INTERVAL_SECONDS)
                    - datetime.datetime.now()
                ).total_seconds()
            )
            printed_warning_already = True
        pass


def _pause_for_pacing(last_call_datetime: datetime.datetime):
    time_since_last_call = datetime.datetime.now() - last_call_datetime
    seconds_since_last_call = time_since_last_call.total_seconds()
    should_pause = seconds_since_last_call < PACING_INTERVAL_SECONDS

    return should_pause
