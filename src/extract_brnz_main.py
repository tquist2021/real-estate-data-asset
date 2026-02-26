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

## Housing Prices
logger.info("Starting Housing Prices")

case_shil_price = get_brnz_extract("CSUSHPINSA")
write_datalake(case_shil_price, "brnz", "case_shill_price_index")

all_trans_price = get_brnz_extract("USSTHPI")
write_datalake(all_trans_price, "brnz","all_trans_price_index")

## Housing Supply
logger.info("Starting Housing Supply")

house_start = get_brnz_extract("HOUST")
write_datalake(house_start, "brnz", "pvt_house_start")

build_permits = get_brnz_extract("PERMIT")
write_datalake(build_permits, "brnz", "build_permits")

house_cmplt = get_brnz_extract("COMPUTSA")
write_datalake(house_cmplt, "brnz", "house_complete")

## Household Leverage 
logger.info("Starting Household Leverage")

hh_debt = get_brnz_extract("CMDEBT")
write_datalake(hh_debt, "brnz","household_debt_all")

home_mort_outstndg = get_brnz_extract("HHMSDODNS")
write_datalake(home_mort_outstndg, "brnz", "home_mortg_outstanding")

hh_debt_serv_pmt = get_brnz_extract("TDSP")
write_datalake(hh_debt_serv_pmt, "brnz", "hh_debt_serv_pmt")

## Mortgage & Credit Stress
logger.info("Starting Mortgage & Credit Stress")

delinq = get_brnz_extract("DRSFRMACBS")
write_datalake(delinq, "brnz", "delinq_rate_sngl_fmly")

loan_officr_survy = get_brnz_extract("DRTSCILM")
write_datalake(loan_officr_survy, "brnz", "loan_officer_survey")

ted = get_brnz_extract("TEDRATE")
write_datalake(ted, "brnz", "ted_spread")

## Interest Rates
logger.info("Starting Interest Rates")

eff = get_brnz_extract("FEDFUNDS")
write_datalake(eff, "brnz", "eff_fed_funds_rate")

treas10 = get_brnz_extract("DGS10")
write_datalake(treas10, "brnz", "treasury_10_yield")

mort30 = get_brnz_extract("MORTGAGE30US")
write_datalake(mort30, "brnz", "mortgage30_rate")

## Macro Controls
logger.info("Starting Macro Controls")

unemp_rate = get_brnz_extract("UNRATE")
write_datalake(unemp_rate, "brnz", "unemployment_rate")

real_gdp = get_brnz_extract("GDPC1")
write_datalake(real_gdp, "brnz", "real_gdp")

disp_pers_income = get_brnz_extract("DSPIC96")
write_datalake(disp_pers_income, "brnz", "disposable_personal_income")

m2 = get_brnz_extract("M2SL")
write_datalake(m2, "brnz", "m2_money_supply")

logger.info("✅ All data were written successfully.")