# 📊 Housing Market & Macro Data Asset (~1971–Present)

A curated set of time series from FRED, suitable for analysis of the 2008 housing crash and future macro research.

---

## 🏠 Housing Prices

| Variable | FRED Code | Notes |
|----------|-----------|-------|
| S&P/Case-Shiller U.S. National Home Price Index | CSUSHPINSA | Captures national housing bubble trends |
| FHFA House Price Index (Purchase-Only) | USSTHPI | Less volatile, cleaner methodology |

---

## 🏗 Housing Supply

| Variable | FRED Code | Notes |
|----------|-----------|-------|
| Housing Starts: Total | HOUST | Leading indicator for housing cycle |
| Building Permits | PERMIT | Early sign of construction trends |
| New Privately Owned Housing Units Completed | COMPUTSA | Measures actual supply hitting the market |

---

## 💳 Household Leverage

| Variable | FRED Code | Notes |
|----------|-----------|-------|
| Household Debt: All Sectors | CMDEBT | Total household leverage |
| Home Mortgages Outstanding | HHMSDODNS | Mortgage debt specifically |
| Household Debt Service Payments (% of Disposable Income) | TDSP | Key metric for household financial stress |

---

## 💥 Mortgage & Credit Stress

| Variable | FRED Code | Notes |
|----------|-----------|-------|
| Delinquency Rate on Single-Family Residential Mortgages | DRSFRMACBS | Measures mortgage defaults |
| Senior Loan Officer Survey – Tightening Standards | DRTSCILM | Leading indicator of bank lending constraints |
| TED Spread | TEDRATE | Measures financial system stress / credit risk |

---

## 🏦 Interest Rates

| Variable | FRED Code | Notes |
|----------|-----------|-------|
| Effective Federal Funds Rate | FEDFUNDS | Short-term monetary policy rate |
| 10-Year Treasury Yield | DGS10 | Long-term risk-free rate |
| 30-Year Fixed Mortgage Rate, Average, United States | MORTGAGE30US | Long-term mortgage borrowing cost; key for affordability and housing demand |

---

## 📊 Macro Controls

| Variable | FRED Code | Notes |
|----------|-----------|-------|
| Unemployment Rate | UNRATE | Labor market conditions |
| Real GDP | GDPC1 | Standard macro control |
| Disposable Personal Income | DSPIC96 | Normalization for debt/payment ratios |
| M2 Money Supply | M2SL | Broad liquidity measure |

---

## 💡 Recommended Data Structure

1. **Prices** → CSUSHPINSA, USSTHPI  
2. **Supply** → HOUST, PERMIT, COMPUTSA  
3. **Household Leverage** → CMDEBT, HHMSDODNS, TDSP  
4. **Credit & Stress** → DRSFRMACBS, DRTSCILM, TEDRATE  
5. **Interest Rates** → FEDFUNDS, DGS10, MORTGAGE30US  
6. **Macro Controls** → UNRATE, GDPC1, DSPIC96, M2SL  

> This structure allows for cross-period comparison, regression analysis, and future-proof macro modeling.