## Buffer class used in both position sizing and portfolio
import pandas as pd

from sysdata.config.configdata import Config
from syslogdiag.logger import logger
from syslogdiag.log_to_screen import logtoscreen
from syscore.objects import arg_not_supplied
from sysdata.futures.virtual_futures_data import virtualFuturesData

def calculate_actual_buffers(
            buffers: pd.DataFrame,
            cap_multiplier: pd.Series
    ) -> pd.DataFrame:
    """
    Used when rescaling capital for accumulation
    """

    cap_multiplier = cap_multiplier.reindex(buffers.index).ffill()
    cap_multiplier = pd.concat([cap_multiplier, cap_multiplier], axis=1)
    cap_multiplier.columns = buffers.columns

    actual_buffers_for_position = buffers * cap_multiplier

    return actual_buffers_for_position

def apply_buffers_to_position(position: pd.Series, buffer: pd.Series) -> pd.DataFrame:
    top_position = position.ffill() + buffer.ffill()
    bottom_position = position.ffill() - buffer.ffill()

    pos_buffers = pd.concat([top_position, bottom_position], axis=1)
    pos_buffers.columns = ["top_pos", "bot_pos"]

    return pos_buffers

def calculate_buffers(instrument_code: str,
                      position: pd.Series,
                      config: Config,
                      vol_scalar: pd.Series,
                      daily_prices: pd.Series,
                      raw_costs,
                      instr_weights: pd.DataFrame = arg_not_supplied,
                      idm: pd.Series = arg_not_supplied,
                      log: logger = logtoscreen(""),
    ) -> pd.Series:

    log.msg(
        "Calculating buffers for %s" % instrument_code,
        instrument_code=instrument_code,
    )

    if virtualFuturesData.is_virtual(instrument_code):
        buffer_method = config.virtual_futures_buffer_method
    else:
        buffer_method = config.buffer_method

    if buffer_method == "forecast":
        log.msg(
            "Calculating forecast method buffers for %s" % instrument_code,
            instrument_code=instrument_code,
        )
        if instr_weights is arg_not_supplied:
            instr_weight_this_code = arg_not_supplied
        else:
            instr_weight_this_code = instr_weights[instrument_code]

        buffer = get_forecast_method_buffer(instrument_code=instrument_code,
                                            instr_weight_this_code=instr_weight_this_code,
                                            vol_scalar=vol_scalar,
                                            idm=idm,
                                            position=position,
                                            config=config)

    elif buffer_method == "position":
        log.msg(
            "Calculating position method buffer for %s" % instrument_code,
            instrument_code=instrument_code,
        )

        buffer = get_position_method_buffer(instrument_code=instrument_code, config=config,
                                                 position=position)
    elif buffer_method == "fixed_value":
        log.msg(
            "Calculating fixed value method buffer for %s" % instrument_code,
            instrument_code=instrument_code,
        )
        buffer = get_fixed_value_method_buffer(instrument_code, daily_prices, config=config)
    elif buffer_method == "fixed_cost_ratio":
        log.msg(
            "Calculating fixed cost ratio method buffer for %s" % instrument_code,
            instrument_code=instrument_code,
        )
        buffer = get_fixed_cost_ratio_method_buffer(instrument_code, daily_prices, raw_costs, config=config)
    elif buffer_method == "none":
        log.msg(
            "None method, no buffering for %s" % instrument_code,
            instrument_code=instrument_code,
        )

        buffer = get_buffer_if_not_buffering(
                                                  position=position)
    else:
        log.critical(
            "Buffer method %s not recognised - not buffering" % buffer_method
        )
        buffer = get_buffer_if_not_buffering(
                                                  position=position)

    return buffer


def get_forecast_method_buffer(
                            instrument_code: str,
                            position: pd.Series,
                            vol_scalar: pd.Series,
                            config: Config,
                            instr_weight_this_code: pd.Series = arg_not_supplied,
                            idm: pd.Series = arg_not_supplied,

    ) -> pd.Series:
    """
    Gets the buffers for positions, using proportion of average forecast method


    :param instrument_code: instrument to get values for
    :type instrument_code: str

    :returns: Tx1 pd.DataFrame
    """


    if virtualFuturesData.is_virtual(instrument_code):
        buffer_size = config.virtual_futures_buffer_size
    else:
        buffer_size = config.buffer_size

    buffer = _calculate_forecast_buffer_method(
        buffer_size=buffer_size,
        position=position,
        idm=idm,
        instr_weight_this_code=instr_weight_this_code,
        vol_scalar=vol_scalar,
    )

    return buffer


def get_position_method_buffer(
                               instrument_code: str,
                               position: pd.Series,
                               config: Config,
                               ) -> pd.Series:
    """
    Gets the buffers for positions, using proportion of position method

    """

    if virtualFuturesData.is_virtual(instrument_code):
        buffer_size = config.virtual_futures_buffer_size
    else:
        buffer_size = config.buffer_size
    abs_position = abs(position)

    buffer = abs_position * buffer_size

    buffer.columns = ["buffer"]

    return buffer


def get_buffer_if_not_buffering(position: pd.Series) -> pd.Series:

    EPSILON_POSITION = 0.001
    buffer = pd.Series([EPSILON_POSITION] * position.shape[0], index=position.index)

    return buffer


def get_fixed_value_method_buffer(instrument_code: str, daily_prices, config: Config) -> pd.Series:
    """
    Gets the buffers for positions, using fixed value method

    :param instrument_code: instrument to get values for
    :type instrument_code: str

    :returns: Tx1 pd.DataFrame
    """


    if virtualFuturesData.is_virtual(instrument_code):
        buffer_size = config.virtual_futures_buffer_size
    else:
        buffer_size = config.buffer_size
    buffer = buffer_size / daily_prices

    buffer.columns = ["buffer"]

    return buffer


def get_fixed_cost_ratio_method_buffer(self, instrument_code: str, daily_prices, raw_costs, config: Config) -> pd.Series:
    """
    Gets the buffers for positions, using method of fixed cost ratio of lot value

    :param instrument_code: instrument to get values for
    :type instrument_code: str

    :returns: Tx1 pd.DataFrame
    """

    max_lot_value = config.fixed_cost_max_lot_value

    """
    cost ratio = cr = trading_cost / lot_value = trading_cost / (n*price)
        = ( slippage*n + per_trade_commission ) / (n*price)
        -->
        buffer = n = per_trade_commission / ( price*cr - slippage) = ptc / div
    """
    cr = config.fixed_cost_ratio
    ptc = raw_costs.value_of_pertrade_commission
    div = daily_prices * cr - raw_costs.price_slippage

    """
        .. But to cap the lot_value to max_lot_value:
        lot_value = n*price < max_lot_value  <-->
        price*(ptc / div) < max_lot_value <-->
        div > price*ptc/max_lot_value
    
        if div <= price*ptc/max_lot_value, cap it -->
        max_buffer = max_lot_value / price
        AND
        max_buffer = ptc / min_div  -->
        min_div = ptc / max_buffer = ptc * price / max_lot_value
    """
    min_div = ptc * daily_prices / max_lot_value
    div[ div <= daily_prices*ptc/max_lot_value ] = min_div

    # finally calculate the buffer size
    buffer = ptc / div

    buffer.columns = ["buffer"]

    return buffer




def _calculate_forecast_buffer_method(
    position: pd.Series,
    buffer_size: float,
    vol_scalar: pd.Series,
    idm: pd.Series = arg_not_supplied,
    instr_weight_this_code: pd.Series = arg_not_supplied,
):

    if instr_weight_this_code is arg_not_supplied:
        instr_weight_this_code_indexed = 1.0
    else:
        instr_weight_this_code_indexed = instr_weight_this_code.reindex(position.index).ffill()

    if idm is arg_not_supplied:
        idm_indexed = 1.0
    else:
        idm_indexed = idm.reindex(position.index).ffill()

    vol_scalar_indexed = vol_scalar.reindex(position.index).ffill()

    average_position = abs(vol_scalar_indexed * instr_weight_this_code_indexed * idm_indexed)

    buffer = average_position * buffer_size

    return buffer