"""
IB connection using ib-insync https://ib-insync.readthedocs.io/api.html

"""

import time

from ib_insync import IB

from sysbrokers.IB.ib_connection_defaults import ib_defaults, alternative_ib_defaults
from syscore.objects import missing_data,arg_not_supplied

from syslogdiag.log_to_screen import logtoscreen

from sysdata.config.production_config import get_production_config

from asyncio.exceptions import TimeoutError
class ClientIdAlreadyInUseError(ConnectionError): ...


class connectionIB(object):
    """
    Connection object for connecting IB
    (A database plug in will need to be added for streaming prices)
    """

    def __init__(
        self,
        client_id: int,
        ib_ipaddress: str = arg_not_supplied,
        ib_port: int = arg_not_supplied,
        account: str = arg_not_supplied,
        log=logtoscreen("connectionIB"),
        alternative_connection=False
    ):
        """
        :param client_id: client id
        :param ipaddress: IP address of machine running IB Gateway or TWS. If not passed then will get from private config file, or defaults
        :param port: Port listened to by IB Gateway or TWS
        :param log: logging object
        :param mongo_db: mongoDB connection
        """
        self._log = log

        # resolve defaults

        if alternative_connection:
            ipaddress, port, __ = alternative_ib_defaults(ib_ipaddress=ib_ipaddress, ib_port=ib_port)
        else:
            ipaddress, port, __ = ib_defaults(ib_ipaddress=ib_ipaddress, ib_port=ib_port)

        # The client id is pulled from a mongo database
        # If for example you want to use a different database you could do something like:
        # connectionIB(mongo_ib_tracker =
        # mongoIBclientIDtracker(database_name="another")

        # You can pass a client id yourself, or let IB find one

        # If you copy for another broker include this line
        log.label(broker="IB", clientid=client_id)
        self._ib_connection_config = dict(
            ipaddress=ipaddress, port=port, client=client_id
        )

        ib = IB()
        ib.client.apiError += self.apiError

        if alternative_connection:
            # connect without account id
            account = ''
        else:
            if account is arg_not_supplied:
                ## not passed get from config
                account = get_broker_account()

            ## that may still return missing data...
            if account is missing_data:
                self.log.error("Broker account ID not found in private config - may cause issues")
                account = ''
            else:
                ## connect using account
                pass

        self._ib = ib
        self._account = account

        self.connect()


    def connect(self):
        self._is_client_id_already_in_use = False
        try:
            self.ib.connect(
                self._ib_connection_config['ipaddress'], 
                self._ib_connection_config['port'],
                clientId=self.client_id(),
                account=self.account)
        except TimeoutError:
            # This exception can be raised when socket connection to gateway is established but it is closed by peer 
            # (because client ID is already in use). However it can also be raised when gateway is down
            if self._is_client_id_already_in_use:
                raise ClientIdAlreadyInUseError("Client id %d already in use!" % self.client_id())
            raise ConnectionRefusedError("Gateway is down")

        except ConnectionRefusedError:
            # This exception is raised when gateway is not responding
            raise ConnectionRefusedError("Gateway is down")

        # Sometimes takes a few seconds to resolve... only have to do this once per process so no biggie
        time.sleep(1)


    def apiError(self, msg):
        # bit hacky but ib_insync doesn't raise a separate Exception for this error so this is an only way to distinguish it
        if "already in use" in msg:
            self._is_client_id_already_in_use = True

    @property
    def ib(self):
        return self._ib

    @property
    def log(self):
        return self._log

    def __repr__(self):
        return "IB broker connection" + str(self._ib_connection_config)

    def client_id(self):
        return self._ib_connection_config["client"]

    @property
    def account(self):
        return self._account

    def close_connection(self):
        self.log.msg("Terminating %s" % str(self._ib_connection_config))
        try:
            # Try and disconnect IB client
            self.ib.disconnect()
        except BaseException:
            self.log.warn(
                "Trying to disconnect IB client failed... ensure process is killed"
            )


def get_broker_account() -> str:
    production_config = get_production_config()
    account_id = production_config.get_element_or_missing_data("broker_account")
    return account_id
