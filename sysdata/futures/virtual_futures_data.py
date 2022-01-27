import pandas as pd
from syscore.fileutils import get_filename_for_package
from sysdata.data_blob import dataBlob
from sysdata.futures.adjusted_prices import futuresAdjustedPrices
from sysobjects.instruments import futuresInstrument
from sysobjects.contracts import futuresContract
from sysobjects.instruments import futuresInstrument
from sysobjects.contract_dates_and_expiries import contractDate
from sysobjects.multiple_prices import futuresMultiplePrices
from sysproduction.data.prices import diagPrices
from sysobjects.dict_of_named_futures_per_contract_prices import setOfNamedContracts
from sysbrokers.IB.ib_instruments_data import ibFuturesInstrumentData
from sysobjects.rolls import rollParameters

VIRTUAL_FUTURES_CONTRACT_DATE = "21000100"
VIRTUAL_FUTURES_CONTRACT_EXPIRATION_DATE = "21000131"

DEFAULT_LOT_VALUE = 5000

class virtualFuturesData(object):

    def __init__():
        pass

    def __repr__(self):
        return "Virtual Futures Data"

    @classmethod
    def get_contract_date(self) -> str:
        return VIRTUAL_FUTURES_CONTRACT_DATE

    @classmethod
    def get_expiration_date(self) -> str:
        return VIRTUAL_FUTURES_CONTRACT_EXPIRATION_DATE

    @classmethod
    def get_lot_size_from_price(self, instrument_code, prices) -> float:
        return DEFAULT_LOT_VALUE / prices[-1]

    @classmethod
    def get_current_contract_dict(self, instrument_code):
        date_str = self.get_virtual_futures_contract(instrument_code).date_str
        return setOfNamedContracts(
            dict({'PRICE':date_str, 'FORWARD':date_str, 'CARRY':date_str})
        )

    @classmethod
    def is_virtual_by_broker_code(self, broker_code: str) -> bool:
        ib_data = ibFuturesInstrumentData(ibconnection=None)
        try: 
            instr_code = ib_data.get_instrument_code_from_broker_code(broker_code)
            return self.is_virtual(instr_code)
        except:
            pass
        return False

    @classmethod
    def is_virtual(self, instrument_code: str) -> bool:
        if instrument_code[:2] == 'V_':
            return True
        return False

    @classmethod
    def get_virtual_futures_contract(self, instrument_code):
        return futuresContract( futuresInstrument(instrument_code), contractDate(VIRTUAL_FUTURES_CONTRACT_DATE) )

    @classmethod
    def get_prices(self, data:dataBlob, instrument_code):
        diag_prices = diagPrices(data)
        contract = self.get_virtual_futures_contract(instrument_code)
        price_series = diag_prices.get_prices_for_contract_object(contract)
        price_series = price_series[~price_series.index.duplicated()]
        return price_series


    @classmethod
    def get_adjusted_prices(self, data:dataBlob, instrument_code):
        price_series = self.get_prices(data, instrument_code)
        return futuresAdjustedPrices(price_series['FINAL'])


    @classmethod
    def get_multiple_prices(self, data:dataBlob, instrument_code):
        price_series = self.get_prices(data, instrument_code)
        p = pd.DataFrame( index=price_series.index, columns=['PRICE', 'CARRY', 'FORWARD', 'PRICE_CONTRACT', 'CARRY_CONTRACT', 'FORWARD_CONTRACT'])
        p['PRICE'] = price_series['FINAL']
        p['PRICE_CONTRACT'] = VIRTUAL_FUTURES_CONTRACT_DATE
        p['FORWARD'] = price_series['FINAL']
        p['FORWARD_CONTRACT'] = VIRTUAL_FUTURES_CONTRACT_DATE
        return futuresMultiplePrices( p )


    @classmethod
    def get_roll_parameters(self, instrument_code):
        return rollParameters(
            hold_rollcycle='Z',
            priced_rollcycle='Z',
            roll_offset_day=0,
            carry_offset=0,
            approx_expiry_offset=31           
            )
