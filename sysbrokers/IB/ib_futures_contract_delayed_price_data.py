from syscore.dateutils import Frequency
from syscore.objects import missing_contract, missing_data, no_market_permissions
from sysobjects.futures_per_contract_prices import futuresContractPrices
from sysbrokers.IB.client.ib_price_client import IB_MARKET_DATE_TYPE__DELAYED, IB_MARKET_DATE_TYPE__LIVE
from sysexecution.tick_data import dataFrameOfRecentTicks
from sysobjects.contracts import futuresContract
from sysbrokers.IB.ib_futures_contract_price_data import ibFuturesContractPriceData
import datetime
import pandas as pd

def from_ib_delayed_bid_ask_tick_data_to_dataframe(tick_data) -> dataFrameOfRecentTicks:
    """

    :param tick_data: ticker
    :return: pd.DataFrame,['priceBid', 'priceAsk', 'sizeAsk', 'sizeBid']
    """
    time_index = [tick_data.time]
    fields = {
        "priceBid": "bid",
        "priceAsk": "ask",
        "sizeAsk": "askSize",
        "sizeBid": "bidSize"
    }

    value_dict = {}
    for field_name in fields.keys():
            field_values = [getattr(tick_data, fields.get(field_name))]
            value_dict[field_name] = field_values

    output = dataFrameOfRecentTicks(value_dict, time_index)

    return output

class ibFuturesContractDelayedPriceData(ibFuturesContractPriceData):

    def __repr__(self):
        return "IB Futures per contract price data (delayed) %s" % str(self.ib_client)

    def get_recent_bid_ask_tick_data_for_contract_object(
        self, contract_object: futuresContract
    ) -> dataFrameOfRecentTicks:
        """
        Get last few price ticks using delayed data

        :param contract_object: futuresContract
        :return:
        """
        new_log = contract_object.log(self.log)

        contract_object_with_ib_data = (
            self.futures_contract_data.get_contract_object_with_IB_data(contract_object)
        )
        if contract_object_with_ib_data is missing_contract:
            new_log.warn("Can't get data for %s" % str(contract_object))
            return dataFrameOfRecentTicks.create_empty()

        self.ib_client.set_market_data_type(IB_MARKET_DATE_TYPE__DELAYED)
        tick_data = self.ib_client.ib_get_delayed_bid_ask_tick_data(
            contract_object_with_ib_data
        )
        self.ib_client.set_market_data_type(IB_MARKET_DATE_TYPE__LIVE)

        if tick_data is missing_data or tick_data is missing_contract:
            return missing_data

        if tick_data is no_market_permissions:
            return no_market_permissions

        if tick_data.bid < 0 and tick_data.ask < 0:
            # it's outside trading hours
            return missing_data

        tick_data_as_df = from_ib_delayed_bid_ask_tick_data_to_dataframe(tick_data)

        return tick_data_as_df

    def _get_prices_at_frequency_for_contract_object_no_checking(
        self,
        contract_object: futuresContract,
        freq: Frequency,
        startDateTime="",
        allow_expired=False,
    ) -> futuresContractPrices:

        """
        Get spoofed historical prices using delayed last tick price. 
        Only one (last) price is returned. Frequency is ignored.

        :param contract_object:  futuresContract
        :return: data
        """
        new_log = contract_object.log(self.log)

        contract_object_with_ib_broker_config = (
            self.futures_contract_data.get_contract_object_with_IB_data(
                contract_object, allow_expired=allow_expired
            )
        )
        if contract_object_with_ib_broker_config is missing_contract:
            new_log.warn("Can't get data for %s" % str(contract_object))
            return futuresContractPrices.create_empty()

        self.ib_client.set_market_data_type(IB_MARKET_DATE_TYPE__DELAYED)
        tick_data = self.ib_client.ib_get_delayed_bid_ask_tick_data(
            contract_object_with_ib_broker_config
        )
        self.ib_client.set_market_data_type(IB_MARKET_DATE_TYPE__LIVE)

        if tick_data is missing_data:
            return futuresContractPrices.create_empty()

        if tick_data.bid < 0 and tick_data.ask < 0:
            # it's outside trading hours so there's only the last closing price data available.
            # Unfortunately IB doesn't give the actual closing datetime and we cannot decipher this
            # from other reliable source so we have to give up
            return futuresContractPrices.create_empty()

        # Price is delayed by 15-20 minutes
        last_trade = pd.Timestamp(tick_data.time - datetime.timedelta(minutes=20))
        last_trade_local_time = self.ib_client._adjust_ib_time_to_local(last_trade)

        price_data = pd.DataFrame(data={ 
            "OPEN":tick_data.open, 
            "HIGH":tick_data.high, 
            "LOW":tick_data.low, 
            "FINAL":tick_data.last,
            "VOLUME":tick_data.volume},
            index=[last_trade_local_time]
        )

        price_data = futuresContractPrices(price_data)

        return price_data
