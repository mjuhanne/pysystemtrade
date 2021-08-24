"""
Read data from Norgate for individual futures contracts. 

Requires 'norgatedata' module (please see https://pypi.org/project/norgatedata/ )

"""
import datetime
import norgatedata
from sysdata.norgate.ng_database import norgateInstrumentDatabase
from sysdata.futures.futures_per_contract_prices import futuresContractPriceData, listOfFuturesContracts
from sysobjects.futures_per_contract_prices import futuresContractPrices
from sysobjects.contracts import futuresContract
from syslogdiag.log_to_screen import logtoscreen
from syscore.dateutils import month_from_contract_letter
from syscore.dateutils import Frequency, DAILY_PRICE_FREQ
from sysobjects.contract_dates_and_expiries import YEAR_SLICE
import numpy as np
from syscore.objects import missing_instrument


class norgateFuturesContractPriceData(futuresContractPriceData):
    """
    Class to read futures price data from Norgate data (https://norgatedata.com/)
    """

    def __init__(self,
                 log=logtoscreen("norgateFuturesContractPriceData")):

        super().__init__(log=log)
        self.db = norgateInstrumentDatabase()


    def __repr__(self):
        return "Norgate Futures per contract price data"


    def _get_prices_for_contract_object_no_checking(self,
            futures_contract_object: futuresContract, include_open_interest = False ) -> futuresContractPrices:
        """
        Read back the prices for a given contract object

        :param contract_object:  futuresContract
        :param include_open_interest: bool For future reference that OI data is available from Norgate (not used currently)
        :return: data
        """
        #start_date = pd.Timestamp(datetime.date.today()) - pd.Timedelta(365,'D')
        timeseriesformat = 'pandas-dataframe'
        ng_code = self.db.get_ng_id(futures_contract_object.instrument_code)
        if ng_code is missing_instrument:
            self.log.warning("Can't find instrument %s from Norgate database!" % futures_contract_object.instrument_code)
            return missing_instrument

        contract = ng_code \
            + "-" + futures_contract_object.contract_date.date_str[YEAR_SLICE] \
            + futures_contract_object.contract_date.letter_month()
        df = norgatedata.price_timeseries(symbol=contract,
            #start_date=start_date,
            timeseriesformat=timeseriesformat)

        if len(df.index) > 0:
            df.rename( columns = {
                "Open" : "OPEN",
                "High" : "HIGH",
                "Low" : "LOW",
                "Close" : "FINAL",
                "Volume" : "VOLUME" },
                errors="raise",
                inplace=True
            )
            if include_open_interest == True:
                df.rename( columns = {
                    "Open Interest" : "OPEN_INTEREST" },
                    errors="raise",
                    inplace=True
                )
            else:
                df.drop(["Open Interest"], axis=1, errors="raise", inplace=True)

            # Do unit conversion if needed
            mult = self.db.get_unit_multiplier(futures_contract_object.instrument_code)
            if mult != 1:
                df["OPEN"] = df["OPEN"] * mult
                df["HIGH"] = df["HIGH"] * mult
                df["LOW"] = df["LOW"] * mult
                df["FINAL"] = df["FINAL"] * mult

            # Append date with time (23:00:00) 
            p_datetime = df.index.values.copy()
            p_datetime = p_datetime + np.timedelta64(23,'h')
            df.index = p_datetime

            fcp = futuresContractPrices(df)
        else:
            fcp = futuresContractPrices.create_empty()

        return fcp


    def _write_prices_for_contract_object_no_checking(self,
                                                      futures_contract_object: futuresContract,
                                                      futures_price_data: futuresContractPrices):
        raise NotImplementedError                                                    


    def contracts_with_price_data_for_instrument_code(self,
                                                      instrument_code: str, 
                                                      allow_expired = True) -> listOfFuturesContracts:
        """
        Get all contracts that have price data for given instrument

        :param instrument_code:  Instrument code
        :param allow_expired: bool Exclude contracts that have their expiration date passed (approximate with current date and contract date)
        :return: data
        """
        symbol = self.db.get_ng_id(instrument_code)
        if symbol is missing_instrument:
            return missing_instrument

        contract_list = norgatedata.futures_market_session_contracts(symbol)
        list_of_contracts = []

        for contract in contract_list:
            symbol = contract.split("-")[0]
            timecode = contract.split("-")[1]
            year = int(timecode[0:-1])
            monthcode = timecode[-1]
            month = month_from_contract_letter(monthcode)
            contract_date = "%04d%02d%02d" % (year,month,0)

            fc = futuresContract.from_two_strings(instrument_code=instrument_code,contract_date_str=contract_date )

            if allow_expired == False:
                # we don't actually know if the contract is expired, but we
                # can try to guess this from the contract date
                e_date = fc.expiry_date # derived automatically from contract date
                if e_date + datetime.timedelta(days=31) > datetime.datetime.now():
                    list_of_contracts.append(fc)
            else:
                list_of_contracts.append(fc)

        return listOfFuturesContracts(list_of_contracts)


    def get_contracts_with_price_data(self) -> listOfFuturesContracts:
        """
        :return: list of contracts
        """
        list_of_contracts = listOfFuturesContracts()
        instruments = self.db.get_list_of_instruments()
        for instr in instruments:
            contracts = self.contracts_with_price_data_for_instrument_code(instr)
            list_of_contracts += contracts
            
        return list_of_contracts


    def has_data_for_contract(self, contract_object: futuresContract) ->bool:
        fcp = self._get_prices_for_contract_object_no_checking(contract_object)
        if len(fcp.index) > 0:
            return True
        else:
            return False

    
    def get_prices_for_contract_object(self, contract_object: futuresContract,
        include_open_interest = False ):
        """
        Get all prices for a given contract object

        :param contract_object:  futuresContract
        :param include_open_interest: bool For future reference that OI data is available from Norgate (not used currently)
        :return: data
        """
        return self._get_prices_for_contract_object_no_checking(contract_object, include_open_interest)


    def get_prices_at_frequency_for_contract_object(
            self, contract_object: futuresContract, include_open_interest = False,
            freq: Frequency = DAILY_PRICE_FREQ):
        if freq is not DAILY_PRICE_FREQ:
            raise NotImplementedError
        return self._get_prices_for_contract_object_no_checking(contract_object, include_open_interest)


    def get_prices_at_frequency_for_potentially_expired_contract_object(
            self, contract_object: futuresContract, include_open_interest = False, 
            freq: Frequency = DAILY_PRICE_FREQ):
        if freq is not DAILY_PRICE_FREQ:
            raise NotImplementedError
        return self._get_prices_for_contract_object_no_checking(contract_object, include_open_interest)


    def _delete_prices_for_contract_object_with_no_checks_be_careful(
            self, futures_contract_object: futuresContract):
       raise NotImplementedError


 