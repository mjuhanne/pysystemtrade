from dateutil.tz import tz
import datetime
from time import sleep
from ib_insync import Contract
from ib_insync import IB

from sysbrokers.IB.ib_connection import connectionIB

from syscore.dateutils import strip_timezone_fromdatetime
from syslogdiag.logger import logger
from syslogdiag.log_to_screen import logtoscreen

# IB state that pacing violations only occur for bar sizes of less than 1 minute
# See footnote at bottom of
# https://interactivebrokers.github.io/tws-api/historical_limitations.html#pacing_violations
PACING_INTERVAL_SECONDS = 0.5


STALE_SECONDS_ALLOWED_ACCOUNT_SUMMARY = 600

IB_ERROR__NO_MARKET_PERMISSIONS = 10187
IB_ERROR__INVALID_CONTRACT      = 200

IB_ERROR_TYPES = {IB_ERROR__INVALID_CONTRACT: "invalid_contract", IB_ERROR__NO_MARKET_PERMISSIONS: "no market permissions"}
IB_IS_ERROR = [IB_ERROR__INVALID_CONTRACT, IB_ERROR__NO_MARKET_PERMISSIONS]


def reconnect(func):
    def wrapper(self, *args, **kwargs):
        # Here we attempt to reconnect to gateway if connection has died
        while 1:
            try:
                return func(self, *args, **kwargs)
            except ConnectionError as e:
                print("reconnect wrapper: ConnectionError (%s)" % (str(e)))
                if self.ib_connection.ib.isConnected():
                    print("ConnectionError exception but client still connected. Retrying..")
                else:
                    self.log.warn("Connection to gateway closed prematurely! Reconnecting..")
                    while 1:
                        attempts = 0
                        try:
                            self.ib_connection.connect()
                            self.log.warn("Connection re-established!")
                            # Connection succeeded! Lets wait a bit just in case client needs some time to start
                            sleep(5)
                            break
                            
                        except ConnectionRefusedError:
                            # This exception is raised when gateway is not responding
                            self.log.warn("Gateway not running! Retrying in 10 seconds..")
                            sleep(10)
                            attempts += 1
                            if attempts == 60*5/10:
                                # After 5 minutes log and notify user by e-mail
                                self.log.critical("Error! Gateway still not running after 5 minutes of down time. Continuing to reconnect..")
    return wrapper


class ibClient(object):
    """
    Client specific to interactive brokers

    We inherit from this to do interesting stuff, so this base class just offers error handling and get time

    """

    def __init__(
        self, ibconnection: connectionIB, log: logger = logtoscreen("ibClient")
    ):

        # means our first call won't be throttled for pacing
        self.last_historic_price_calltime = (
            datetime.datetime.now() - datetime.timedelta(seconds=PACING_INTERVAL_SECONDS)
        )

        # Add error handler
        ibconnection.ib.errorEvent += self.error_handler

        self._ib_connection = ibconnection
        self._log = log
        self.log.label(clientid=ibconnection.client_id())
        self._last_errors = dict()

    @property
    def ib_connection(self) -> connectionIB:
        return self._ib_connection

    @property
    def ib(self) -> IB:
        return self.ib_connection.ib

    @property
    def client_id(self) -> int:
        return self.ib.client.clientId

    @property
    def log(self):
        return self._log

    def error_handler(
        self, reqid: int, error_code: int, error_string: str, contract: Contract
    ):
        """
        Error handler called from server
        Needs to be attached to ib connection

        :param reqid: IB reqid
        :param error_code: IB error code
        :param error_string: IB error string
        :param contract: IB contract or None
        :return: success
        """
        if contract is None:
            contract_str = ""
        else:
            contract_str = " (%s/%s)" % (
                contract.symbol,
                contract.lastTradeDateOrContractMonth,
            )
            self._last_errors[contract.conId] = error_code

        msg = "Reqid %d: %d %s %s" % (reqid, error_code, error_string, contract_str)

        iserror = error_code in IB_IS_ERROR
        if iserror:
            # Serious requires some action
            myerror_type = IB_ERROR_TYPES.get(error_code, "generic")
            self.broker_error(msg, myerror_type)

        else:
            # just a warning / general message
            self.broker_message(msg)


    def get_last_error(self, contract:Contract):
        if contract.conId in self._last_errors:
            return self._last_errors[contract.conId]
        else:
            return None

    def broker_error(self, msg, myerror_type):
        self.log.warn(msg)

    def broker_message(self, msg):
        self.log.msg(msg)

    @reconnect
    def refresh(self):
        self.ib.sleep(0.00001)

    @reconnect
    def get_broker_time_local_tz(self) -> datetime.datetime:
        ib_time = self.ib.reqCurrentTime()
        local_ib_time_with_tz = ib_time.astimezone(tz.tzlocal())
        local_ib_time = strip_timezone_fromdatetime(local_ib_time_with_tz)

        return local_ib_time


