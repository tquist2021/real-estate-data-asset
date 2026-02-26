from utils import get_brnz_extract, write_datalake
import pandas as pd
import logging

# Setup Logging 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()     
    ]
)
logger = logging.getLogger(__name__)

logger.info("Starting Bronze Extract")
try:
    ## Housing Prices
    logger.info("Starting Housing Prices")

    case_shil_price = get_brnz_extract("CSUSHPINSA")
    write_datalake(case_shil_price, "brnz", "hpi_case_shiller_us")

    all_trans_price = get_brnz_extract("USSTHPI")
    write_datalake(all_trans_price, "brnz","hpi_all_transactions_us")

    ## Housing Supply
    logger.info("Starting Housing Supply")

    house_start = get_brnz_extract("HOUST")
    write_datalake(house_start, "brnz", "housing_private_starts")

    build_permits = get_brnz_extract("PERMIT")
    write_datalake(build_permits, "brnz", "housing_building_permits")

    house_cmplt = get_brnz_extract("COMPUTSA")
    write_datalake(house_cmplt, "brnz", "housing_completions")

    ## Household Leverage 
    logger.info("Starting Household Leverage")

    hh_debt = get_brnz_extract("CMDEBT")
    write_datalake(hh_debt, "brnz","hh_total_debt")

    home_mort_outstndg = get_brnz_extract("HHMSDODNS")
    write_datalake(home_mort_outstndg, "brnz", "hh_mortgage_outstanding")

    hh_debt_serv_pmt = get_brnz_extract("TDSP")
    write_datalake(hh_debt_serv_pmt, "brnz", "hh_debt_service_payments")

    ## Mortgage & Credit Stress
    logger.info("Starting Mortgage & Credit Stress")

    delinq = get_brnz_extract("DRSFRMACBS")
    write_datalake(delinq, "brnz", "mortgage_delinquency_single_family")

    loan_officr_survy = get_brnz_extract("DRTSCILM")
    write_datalake(loan_officr_survy, "brnz", "loan_officer_survey")

    ted = get_brnz_extract("TEDRATE")
    write_datalake(ted, "brnz", "ted_spread")

    ## Interest Rates
    logger.info("Starting Interest Rates")

    eff = get_brnz_extract("FEDFUNDS")
    write_datalake(eff, "brnz", "interest_effective_fed_funds")

    treas10 = get_brnz_extract("DGS10")
    write_datalake(treas10, "brnz", "interest_treasury_10y_yield")

    mort30 = get_brnz_extract("MORTGAGE30US")
    write_datalake(mort30, "brnz", "interest_30y_mortgage")

    ## Macro Controls
    logger.info("Starting Macro Controls")

    unemp_rate = get_brnz_extract("UNRATE")
    write_datalake(unemp_rate, "brnz", "labor_unemployment_rate")

    real_gdp = get_brnz_extract("GDPC1")
    write_datalake(real_gdp, "brnz", "macro_real_gdp")

    disp_pers_income = get_brnz_extract("DSPIC96")
    write_datalake(disp_pers_income, "brnz", "income_disposable_personal")

    m2 = get_brnz_extract("M2SL")
    write_datalake(m2, "brnz", "monetary_m2")
    logger.info("✅ All data were written successfully.")
except Exception as e:
    logger.exception(f"🤮 Job failed: {e}")