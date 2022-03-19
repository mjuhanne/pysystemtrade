from sysdata.futures.virtual_futures_data import virtualFuturesData
from sysdata.sim.futures_sim_data import futuresSimData

from sysdata.futures.adjusted_prices import futuresAdjustedPricesData
from sysdata.fx.spotfx import fxPricesData
from sysdata.futures.instruments import futuresInstrumentData
from sysdata.futures.multiple_prices import futuresMultiplePricesData
from sysdata.futures.futures_per_contract_prices import futuresContractPriceData
from sysdata.futures.rolls_parameters import rollParametersData
from sysdata.data_blob import dataBlob
from sysobjects.contracts import futuresContract
from syscore.objects import missing_data

from sysobjects.instruments import (
    assetClassesAndInstruments,
    futuresInstrumentWithMetaData,
)
from sysobjects.spot_fx_prices import fxPrices
from sysobjects.adjusted_prices import futuresAdjustedPrices
from sysobjects.multiple_prices import futuresMultiplePrices
from sysobjects.rolls import rollParameters

from sysobjects.futures_per_contract_prices import futuresContractPrices
import pandas as pd
import numpy as np

class genericBlobUsingFuturesSimData(futuresSimData):
    """
    dataBlob must have the appropriate classes added or it won't work
    """

    def __init__(self, data: dataBlob):
        super().__init__(log=data.log)
        self._data = data
        self._cached_backadjusted_prices = dict()
        self._cached_instrument_asset_classes = None
        self._cached_fx_prices = dict()
        self._cached_multiple_prices = dict()

    @property
    def data(self):
        return self._data

    @property
    def db_fx_prices_data(self) -> fxPricesData:
        return self.data.db_fx_prices

    @property
    def db_futures_adjusted_prices_data(self) -> futuresAdjustedPricesData:
        return self.data.db_futures_adjusted_prices

    @property
    def db_futures_contract_intraday_prices_data(self) -> futuresContractPriceData:
        if not hasattr(self.data, "db_futures_contract_intraday_price"):
            return missing_data
        return self.data.db_futures_contract_intraday_price

    @property
    def db_futures_instrument_data(self) -> futuresInstrumentData:
        return self.data.db_futures_instrument

    @property
    def db_futures_multiple_prices_data(self) -> futuresMultiplePricesData:
        return self.data.db_futures_multiple_prices

    @property
    def db_roll_parameters(self) -> rollParametersData:
        return self.data.db_roll_parameters

    def get_instrument_list(self):
        return self.db_futures_adjusted_prices_data.get_list_of_instruments() \
            + virtualFuturesData.get_list_of_virtual_futures_instruments_with_price_data(self.data)

    def  _get_fx_data_for_date_range(self, currency1: str, currency2: str,
                                      start_date, end_date) -> fxPrices:
        fx_code = currency1+currency2
        if fx_code in self._cached_fx_prices:
            data = self._cached_fx_prices[fx_code]
        else:
            data = self.db_fx_prices_data.get_fx_prices(fx_code)
            self._cached_fx_prices[fx_code] = data

        data_in_date_range =data[start_date:end_date]

        return data_in_date_range

    def get_instrument_asset_classes(self) -> assetClassesAndInstruments:
        if self._cached_instrument_asset_classes is not None:
            return self._cached_instrument_asset_classes
        all_instrument_data = self.get_all_instrument_data_as_df()
        asset_classes = all_instrument_data["AssetClass"]
        asset_class_data = assetClassesAndInstruments.from_pd_series(asset_classes)
        self._cached_instrument_asset_classes = asset_class_data

        return asset_class_data

    def get_all_instrument_data_as_df(self):
        all_instrument_data = (
            self.db_futures_instrument_data.get_all_instrument_data_as_df()
        )
        instrument_list= self.get_instrument_list()
        all_instrument_data = all_instrument_data[all_instrument_data.index.isin(instrument_list)]

        return all_instrument_data


    def append_backadjusted_prices_with_last_intraday_price(self, instrument_code: str, adjusted_prices:futuresAdjustedPrices):
        # extract the current contract
        if virtualFuturesData.is_virtual(instrument_code):
            contract_intraday_prices = virtualFuturesData.get_intraday_prices(self.data, instrument_code)
        else:
            multiple_prices = self.db_futures_multiple_prices_data.get_multiple_prices(instrument_code)
            last_row = multiple_prices.iloc[-1,]
            contract_date = last_row['PRICE_CONTRACT']
            contract = futuresContract.from_two_strings(instrument_code, contract_date)
            contract_intraday_prices = self.db_futures_contract_intraday_prices_data.get_prices_for_contract_object(contract)
        
        if len(contract_intraday_prices) == 0:
            return adjusted_prices
        last_intraday_price = contract_intraday_prices['FINAL'].iloc[-1]
        last_priced_contract_datetime = contract_intraday_prices.index[-1]
        if last_priced_contract_datetime < adjusted_prices.index[-1]:
            print("Warning! Daily prices older than adjusted prices for %s!" % (instrument_code))
            return adjusted_prices
        new_row = pd.Series( data = [last_intraday_price], index=[last_priced_contract_datetime] )
        adjusted_prices = adjusted_prices.append(new_row)
        return adjusted_prices

    def get_backadjusted_futures_price(
        self, instrument_code: str
    ) -> futuresAdjustedPrices:
        if instrument_code in self._cached_backadjusted_prices:
            return self._cached_backadjusted_prices[instrument_code]
        if virtualFuturesData.is_virtual(instrument_code):
            data = virtualFuturesData.get_adjusted_prices(self.data, instrument_code)
        else:
            data = self.db_futures_adjusted_prices_data.get_adjusted_prices(instrument_code)

        if hasattr(self.parent.config,'append_prices_with_last_intraday_price'):
            if self.parent.config.append_prices_with_last_intraday_price:
                if self.db_futures_contract_intraday_prices_data is not missing_data:
                    data = self.append_backadjusted_prices_with_last_intraday_price(instrument_code, data)
        self._cached_backadjusted_prices[instrument_code] = data
        return data

    def fetch_contract_prices(self, instrument_code, contract_date):
        contract = futuresContract.from_two_strings(instrument_code, contract_date)
        if virtualFuturesData.is_virtual(instrument_code):
            contract_intraday_prices = virtualFuturesData.get_intraday_prices(self.data, instrument_code)
        else:
            contract_intraday_prices = self.db_futures_contract_intraday_prices_data.get_prices_for_contract_object(contract)
        return contract_intraday_prices

    def append_multiple_prices_with_last_intraday_price(self, instrument_code: str, multiple_prices:futuresMultiplePrices):
        last_row = multiple_prices.iloc[-1,]

        # priced contract
        priced_contract = last_row['PRICE_CONTRACT']
        priced_contract_intraday_prices = self.fetch_contract_prices(instrument_code, priced_contract)
        if len(priced_contract_intraday_prices) == 0:
            return multiple_prices
        priced_contract_last_price = priced_contract_intraday_prices['FINAL'].iloc[-1]
        last_datetime = priced_contract_intraday_prices.index[-1]
        if last_datetime < multiple_prices.index[-1]:
            print("Warning! Daily prices older than multiple prices for %s!" % (instrument_code))
            return multiple_prices

        # carry contract
        carry_contract = last_row['CARRY_CONTRACT']
        carry_contract_intraday_prices = self.fetch_contract_prices(instrument_code, carry_contract)
        try:
            carry_contract_last_price = carry_contract_intraday_prices.loc[last_datetime, 'FINAL']
        except:
            carry_contract_last_price = np.nan

        data = {
            'PRICE' : [priced_contract_last_price],
            'CARRY' : [carry_contract_last_price],
            'FORWARD' : [np.nan],
            'PRICE_CONTRACT' : [priced_contract],
            'CARRY_CONTRACT' : [carry_contract],
            'FORWARD_CONTRACT' : [np.nan],
        }
        p = pd.DataFrame( index=[last_datetime], data=data )

        p = multiple_prices.append(p)
        return p


    def get_multiple_prices_for_date_range(self, instrument_code: str,
                                            start_date, end_date) -> futuresMultiplePrices:
        if instrument_code in self._cached_multiple_prices:
            data = self._cached_multiple_prices[instrument_code]
        else:
            if virtualFuturesData.is_virtual(instrument_code):
                data = virtualFuturesData.get_multiple_prices(self.data, instrument_code)
            else:
                data = self.db_futures_multiple_prices_data.get_multiple_prices(instrument_code)

            if hasattr(self.parent.config,'append_prices_with_last_intraday_price'):
                if self.parent.config.append_prices_with_last_intraday_price:
                    if self.db_futures_contract_intraday_prices_data is not missing_data:
                        data = self.append_multiple_prices_with_last_intraday_price(instrument_code, data)
            self._cached_multiple_prices[instrument_code] = data

        return data[start_date:end_date]

    def get_instrument_meta_data(
        self, instrument_code: str
    ) -> futuresInstrumentWithMetaData:
        ## cost and other meta data stored in the same place
        return self.get_instrument_object_with_meta_data(instrument_code)

    def get_instrument_object_with_meta_data(
        self, instrument_code: str
    ) -> futuresInstrumentWithMetaData:
        instrument = self.db_futures_instrument_data.get_instrument_data(
            instrument_code
        )

        return instrument

    def get_roll_parameters(self, instrument_code: str) -> rollParameters:
        roll_parameters = self.db_roll_parameters.get_roll_parameters(instrument_code)

        return roll_parameters
