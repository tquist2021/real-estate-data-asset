import os
import pandas as pd
from utils import clean_data, get_table_names, write_datalake
import logging
from dotenv import load_dotenv

# -------------------- Setup Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()     
    ]
)
logger = logging.getLogger(__name__)

# -------------------- Load Environment --------------------
load_dotenv()
data_lake_file_path = os.getenv("data_lake_fp")

logger.info("Starting transformations for Silver tables.")

try:
# -------------------- prices table --------------------
    logger.info("Starting creation of prices Silver table.")
    case_shill_df = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_hpi_case_shiller_us.csv")
    case_shill_df = clean_data(case_shill_df, "date", "case_shill_prc_index")

    fhfa_prc_index_df = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_hpi_all_transactions_us.csv")
    fhfa_prc_index_df = clean_data(fhfa_prc_index_df, "date", "fhfa_prc_index")

    prices_df = pd.merge(case_shill_df, fhfa_prc_index_df, on = "date", how = "left")

    try:
        write_datalake(prices_df, "slvr", "hpi_us")
        logger.info("✅ prices table loaded.")
    except Exception as e:
        logger.exception(f"❌ write failed: {e}")

    # -------------------- supply table --------------------
    logger.info("Starting creation of supply Silver table.")

    pvt_house_start = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_housing_private_starts.csv")
    pvt_house_start = clean_data(pvt_house_start, "date", "cnt_house_start")

    build_permits = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_housing_building_permits.csv")
    build_permits = clean_data(build_permits, "date", "cnt_build_permits")

    house_complete = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_housing_completions.csv")
    house_complete = clean_data(house_complete, "date", "cnt_house_complete")

    supply_df = pd.merge(pvt_house_start, build_permits, on = "date", how = "left")
    supply_df = pd.merge(supply_df, house_complete, on = "date", how = "left")

    try:
        write_datalake(supply_df, "slvr", "housing_supply")
        logger.info("✅ supply table loaded.")
    except Exception as e:
        logger.exception(f"❌ write failed: {e}")

    # -------------------- hhld_leverage table --------------------
    logger.info("Starting creation of Household Leverage table.")

    hhld_debt_all = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_hh_total_debt.csv")
    hhld_debt_all = clean_data(hhld_debt_all, "date", "hh_debt_all_amt")

    home_mort = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_hh_mortgage_outstanding.csv")
    home_mort = clean_data(home_mort, "date", "home_mort_outstndg_amt")

    tdsp = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_hh_debt_service_payments.csv")
    tdsp = clean_data(tdsp, "date", "hh_debt_svc_pmt_pct")


    hhld_leverage_df = pd.merge(hhld_debt_all, home_mort, on = "date", how = "left")
    hhld_leverage_df = pd.merge(hhld_leverage_df, tdsp, on = "date", how = "left")

    try:
        write_datalake(hhld_leverage_df, "slvr", "household_leverage")
        logger.info("✅ hhld_leverage table loaded.")
    except Exception as e:
        logger.exception(f"❌ write failed: {e}")

    # -------------------- credit_stress table --------------------
    logger.info("Starting creation of Credit Stress table.")

    sngl_fmly_delinq_rate = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_mortgage_delinquency_single_family.csv")
    sngl_fmly_delinq_rate = clean_data(sngl_fmly_delinq_rate, "date", "sngl_fmly_delinq_rate")

    loan_officr_survy = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_loan_officer_survey.csv")
    loan_officr_survy = clean_data(loan_officr_survy, "date", "survey_score")

    ted = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_ted_spread.csv")
    ted = clean_data(ted, "date", "ted_spread")

    credit_stress = pd.merge(ted, loan_officr_survy, on = "date", how = "left")
    credit_stress = pd.merge(credit_stress, sngl_fmly_delinq_rate, on = "date", how = "left")

    try:
        write_datalake(credit_stress, "slvr", "mortgage_credit_stress")
        logger.info("✅ credit_stress table loaded.")
    except Exception as e:
        logger.exception(f"❌ write failed: {e}")

    # -------------------- interest_rates table --------------------
    logger.info("Starting creation of Interest Rates table.")

    eff = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_interest_effective_fed_funds.csv")
    eff = clean_data(eff, "date", "eff_fed_funds_rate")

    treas10 = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_interest_treasury_10y_yield.csv")
    treas10 = clean_data(treas10, "date", "treas_10_yield_pct")

    mort30 = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_interest_30y_mortgage.csv")
    mort30 = clean_data(mort30, "date", "mort30_rate")

    interest_rates_df = pd.merge(treas10, mort30, on = "date", how = "left")
    interest_rates_df = pd.merge(interest_rates_df, eff, on = "date", how = "left")

    try:
        write_datalake(interest_rates_df, "slvr", "interest_rates")
        logger.info("✅ interest_rates table loaded.")
    except Exception as e:
        logger.exception(f"❌ write failed: {e}")

    # -------------------- macro_controls table --------------------
    logger.info("Starting creation of macro_controls table")

    unemp_rate = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_labor_unemployment_rate.csv")
    unemp_rate = clean_data(unemp_rate, "date", "unemp_rate")

    real_gdp = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_macro_real_gdp.csv")
    real_gdp = clean_data(real_gdp, "date", "gdp")

    pers_disposable_income = pd.read_csv(f"{data_lake_file_path}/brnz/brnz_income_disposable_personal.csv")
    pers_disposable_income = clean_data(pers_disposable_income, "date", "personal_disposable_income")

    macro_controls_df = pd.merge(unemp_rate, pers_disposable_income, on = "date", how = "left")
    macro_controls_df = pd.merge(macro_controls_df, real_gdp, on = "date", how = "left")

    try:
        write_datalake(macro_controls_df, "slvr", "macro_controls")
        logger.info("✅ macro_controls table loaded.")
    except Exception as e:
        logger.exception(f"❌ write failed: {e}")

    # -------------------- end job --------------------
    logger.info("⭐️ All files written succesfully!")
except Exception as e: 
    logger.exception(f"❌ Write Failed: {e}")