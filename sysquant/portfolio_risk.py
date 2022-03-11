import datetime
import pandas as pd
from syscore.genutils import progressBar

from sysquant.estimators.stdev_estimator import seriesOfStdevEstimates, stdevEstimates
from sysquant.estimators.correlations import (
    correlationEstimate,
    create_boring_corr_matrix,
    CorrelationList,
)
from sysquant.estimators.covariance import (
    covarianceEstimate,
    covariance_from_stdev_and_correlation,
)
from sysquant.optimisation.weights import portfolioWeights, seriesOfPortfolioWeights
from syscore.objects import arg_not_supplied
from systems.multiprocessing import divide_jobs_for_processes
from multiprocessing import Pool

def calc_sum_annualised_risk_given_portfolio_weights(
        portfolio_weights: seriesOfPortfolioWeights,
        pd_of_stdev: seriesOfStdevEstimates) -> pd.Series:

    instrument_list = list(portfolio_weights.columns)
    aligned_stdev = pd_of_stdev[instrument_list].reindex(portfolio_weights.index)

    risk_df = aligned_stdev * portfolio_weights.abs()
    risk_series = risk_df.sum(axis=1)

    return risk_series


def _do_calc_portfolio_risk_series(
        portfolio_weights: seriesOfPortfolioWeights,
        list_of_correlations: CorrelationList,
        pd_of_stdev: seriesOfStdevEstimates,
        show_progressbar: bool,
        relevant_dates: list,
    ) -> list:

    if show_progressbar:
	    progress = progressBar(
	        len(relevant_dates),
	        suffix="Calculating portfolio risk",
	        show_timings=True,
	        show_each_time=False
	    )

        
    risk_series = []
    for relevant_date in relevant_dates:
        if show_progressbar:
            progress.iterate()
        weights_on_date = portfolio_weights.get_weights_on_date(relevant_date)

        covariance = get_covariance_matrix(list_of_correlations = list_of_correlations,
                                           pd_of_stdev = pd_of_stdev,
                                           relevant_date = relevant_date
                                           )
        risk_on_date = weights_on_date.portfolio_stdev(covariance)
        risk_series.append(risk_on_date)

    if show_progressbar:
        progress.finished()
    return risk_series


def calc_portfolio_risk_series(
        portfolio_weights: seriesOfPortfolioWeights,
        list_of_correlations: CorrelationList,
        pd_of_stdev: seriesOfStdevEstimates,
        n_processes,
        show_progressbar: bool) -> pd.Series:

    common_index = list(portfolio_weights.index)
    if n_processes is arg_not_supplied:
        risk_series = _do_calc_portfolio_risk_series(portfolio_weights, list_of_correlations, pd_of_stdev, show_progressbar, common_index)
    else:
        risk_series = []
        args_per_process = divide_jobs_for_processes(n_processes, 
            [portfolio_weights, list_of_correlations, pd_of_stdev, show_progressbar],
            common_index
        )
        with Pool(n_processes) as p:
             for i, partial_risk_series in enumerate(p.starmap(_do_calc_portfolio_risk_series, args_per_process),1):
                 risk_series += partial_risk_series

    risk_series = pd.Series(risk_series, common_index)

    return risk_series

def get_covariance_matrix(list_of_correlations: CorrelationList,
                                           pd_of_stdev: seriesOfStdevEstimates,
                                           relevant_date: datetime.datetime,
                                            ) \
                        -> covarianceEstimate:

    instrument_list = list(pd_of_stdev.columns)
    correlation_estimate = \
        get_correlation_matrix(relevant_date=relevant_date,
                                                  list_of_correlations=list_of_correlations,
                                                  instrument_list = instrument_list)

    stdev_estimate = \
        get_stdev_estimate(relevant_date=relevant_date,
                                        pd_of_stdev = pd_of_stdev)

    covariance = covariance_from_stdev_and_correlation(
        correlation_estimate, stdev_estimate
    )

    return covariance

def get_correlation_matrix(relevant_date: datetime.datetime,
                           list_of_correlations: CorrelationList,
                           instrument_list: list) -> correlationEstimate:
    try:
        correlation_matrix = (
            list_of_correlations.most_recent_correlation_before_date(relevant_date)
        )
    except:
        correlation_matrix = create_boring_corr_matrix(
            len(instrument_list), columns=instrument_list, offdiag=0.0
        )

    return correlation_matrix

def get_stdev_estimate(pd_of_stdev: seriesOfStdevEstimates,
                       relevant_date: datetime.datetime) -> stdevEstimates:
    stdev_estimate = pd_of_stdev.get_stdev_on_date(relevant_date)

    return stdev_estimate