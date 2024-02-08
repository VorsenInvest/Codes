import requests
from datetime import datetime
 
url = "https://brapi.dev/api/quote/WEGE3"
params = {
    'range': '1d',
    'interval': '1d',
    'modules': 'balanceSheetHistory,defaultKeyStatistics,summaryProfile,financialData',
    'token': 'fUNoicPSZTzstUdqw1uVeB',
}


symbol_list = []
marketCap_list = []
type_list = []
longName_list = []
regularMarketChange_list = []
regularMarketChangePercent_list = []
regularMarketTime_list = []
regularMarketPrice_list = []
regularMarketDayHigh_list = []
regularMarketDayRange_list = []
regularMarketDayLow_list = []
regularMarketVolume_list = []
regularMarketOpen_list = []
averageDailyVolume3Month_list = []
averageDailyVolume10Day_list = []
fiftyTwoWeekRange_list = []
fiftyTwoWeekLow_list = []
fiftyTwoWeekHigh_list = []
priceEarnings_list = []
earningsPerShare_list = []
logourl_list = []


enterpriseValue_list = []
forwardPE_list = []
floatShares_list = []
sharesOutstanding_list = []
heldPercentInsiders_list = []
heldPercentInstitutions_list = []
beta_list = []
impliedSharesOutstanding_list = []
bookValue_list = []
priceToBook_list = []
lastFiscalYearEnd_list = []
nextFiscalYearEnd_list = []
mostRecentQuarter_list = []
earningsQuarterlyGrowth_list = []
netIncomeToCommon_list = []
trailingEps_list = []
forwardEps_list = []
pegRatio_list = []
enterpriseToRevenue_list = []
enterpriseToEbitda_list = []
_52WeekChange_list = []
lastDividendValue_list = []
lastDividendDate_list = []

city_list = []
state_list = []
country_list = []
website_list = []

targetHighPrice_list = []
targetLowPrice_list = []
targetMeanPrice_list = []
targetMedianPrice_list = []
recommendationMean_list = []
recommendationKey_list = []
numberOfAnalystOpinions_list = []
totalCash_list = []
totalCashPerShare_list = []
ebitda_list = []
totalDebt_list = []
quickRatio_list = []
currentRatio_list = []
totalRevenue_list = []
debtToEquity_list = []
revenuePerShare_list = []
returnOnAssets_list = []
returnOnEquity_list = []
grossProfits_list = []
freeCashflow_list = []
operatingCashflow_list = []
earningsGrowth_list = []
revenueGrowth_list = []
grossMargins_list = []
ebitdaMargins_list = []
operatingMargins_list = []
profitMargins_list = []
 
response = requests.get(url, params=params)
 
if response.status_code == 200:
    results = response.json()
    print(results)

    first_result = results.get('results')[0]  # Adjust according to the actual JSON structure
    keyStatistics_result = results.get('results')[0].get('defaultKeyStatistics', {}) # Adjust according to the actual JSON structure
    summaryProfile_result = results.get('results')[0].get('summaryProfile', {})  # Adjust according to the actual JSON structure
    financialData_result = results.get('results')[0].get('financialData', {})  # Adjust according to the actual JSON structure


    #print(first_result)
    #print(keyStatistics_result)
    
    print(financialData_result)

    symbol_list.append(first_result.get('symbol'))
    marketCap_list.append(first_result.get('marketCap'))
    type_list.append(' '.join([part for part in first_result.get('shortName').split() if part in ('ON', 'PN')]))
    longName_list.append(first_result.get('longName'))
    regularMarketChange_list.append(first_result.get('regularMarketChange'))
    regularMarketChangePercent_list.append(first_result.get('regularMarketChangePercent'))
    regularMarketTime_list.append('-'.join(first_result.get('regularMarketTime')[:10].split('-')[::-1]))
    regularMarketPrice_list.append(first_result.get('regularMarketPrice'))
    regularMarketDayHigh_list.append(first_result.get('regularMarketDayHigh'))
    regularMarketDayRange_list.append(first_result.get('regularMarketDayRange'))
    regularMarketDayLow_list.append(first_result.get('regularMarketDayLow'))
    regularMarketVolume_list.append(first_result.get('regularMarketVolume'))
    regularMarketOpen_list.append(first_result.get('regularMarketOpen'))
    averageDailyVolume3Month_list.append(first_result.get('averageDailyVolume3Month'))
    averageDailyVolume10Day_list.append(first_result.get('averageDailyVolume10Day'))
    fiftyTwoWeekRange_list.append(first_result.get('fiftyTwoWeekRange'))
    fiftyTwoWeekLow_list.append(first_result.get('fiftyTwoWeekLow'))
    fiftyTwoWeekHigh_list.append(first_result.get('fiftyTwoWeekHigh'))
    priceEarnings_list.append(first_result.get('priceEarnings'))
    earningsPerShare_list.append(first_result.get('earningsPerShare'))
    logourl_list.append(first_result.get('logourl'))



    enterpriseValue_list.append(keyStatistics_result.get('enterpriseValue'))
    forwardPE_list.append(keyStatistics_result.get('forwardPE'))
    floatShares_list.append(keyStatistics_result.get('floatShares'))
    sharesOutstanding_list.append(keyStatistics_result.get('sharesOutstanding'))
    heldPercentInsiders_list.append(keyStatistics_result.get('heldPercentInsiders'))
    heldPercentInstitutions_list.append(keyStatistics_result.get('heldPercentInstitutions'))
    beta_list.append(keyStatistics_result.get('beta'))
    impliedSharesOutstanding_list.append(keyStatistics_result.get('impliedSharesOutstanding'))
    bookValue_list.append(keyStatistics_result.get('bookValue'))
    priceToBook_list.append(keyStatistics_result.get('priceToBook'))
    lastFiscalYearEnd_list.append('-'.join(keyStatistics_result.get('lastFiscalYearEnd')[:10].split('-')[::-1]))
    nextFiscalYearEnd_list.append('-'.join(keyStatistics_result.get('nextFiscalYearEnd')[:10].split('-')[::-1]))
    mostRecentQuarter_list.append('-'.join(keyStatistics_result.get('mostRecentQuarter')[:10].split('-')[::-1]))
    earningsQuarterlyGrowth_list.append(keyStatistics_result.get('earningsQuarterlyGrowth'))
    netIncomeToCommon_list.append(keyStatistics_result.get('netIncomeToCommon'))
    trailingEps_list.append(keyStatistics_result.get('trailingEps'))
    forwardEps_list.append(keyStatistics_result.get('forwardEps'))
    pegRatio_list.append(keyStatistics_result.get('pegRatio'))
    enterpriseToRevenue_list.append(keyStatistics_result.get('enterpriseToRevenue'))
    enterpriseToEbitda_list.append(keyStatistics_result.get('enterpriseToEbitda'))
    _52WeekChange_list.append(keyStatistics_result.get('52WeekChange'))
    lastDividendValue_list.append(keyStatistics_result.get('lastDividendValue'))
    lastDividendDate_list.append('-'.join(keyStatistics_result.get('lastDividendDate')[:10].split('-')[::-1]))

    city_list.append(summaryProfile_result.get('city'))
    state_list.append(summaryProfile_result.get('state'))
    country_list.append(summaryProfile_result.get('country'))
    website_list.append(summaryProfile_result.get('website'))

    targetHighPrice_list.append(financialData_result.get('targetHighPrice'))
    targetLowPrice_list.append(financialData_result.get('targetLowPrice'))
    targetMeanPrice_list.append(financialData_result.get('targetMeanPrice'))
    targetMedianPrice_list.append(financialData_result.get('targetMedianPrice'))
    recommendationMean_list.append(financialData_result.get('recommendationMean'))
    recommendationKey_list.append(financialData_result.get('recommendationKey'))
    numberOfAnalystOpinions_list.append(financialData_result.get('numberOfAnalystOpinions'))
    totalCash_list.append(financialData_result.get('totalCash'))
    totalCashPerShare_list.append(financialData_result.get('totalCashPerShare'))
    ebitda_list.append(financialData_result.get('ebitda'))
    totalDebt_list.append(financialData_result.get('totalDebt'))
    quickRatio_list.append(financialData_result.get('quickRatio'))
    currentRatio_list.append(financialData_result.get('currentRatio'))
    totalRevenue_list.append(financialData_result.get('totalRevenue'))
    debtToEquity_list.append(financialData_result.get('debtToEquity'))
    revenuePerShare_list.append(financialData_result.get('revenuePerShare'))
    returnOnAssets_list.append(financialData_result.get('returnOnAssets'))
    returnOnEquity_list.append(financialData_result.get('returnOnEquity'))
    grossProfits_list.append(financialData_result.get('grossProfits'))
    freeCashflow_list.append(financialData_result.get('freeCashflow'))
    operatingCashflow_list.append(financialData_result.get('operatingCashflow'))
    earningsGrowth_list.append(financialData_result.get('earningsGrowth'))
    revenueGrowth_list.append(financialData_result.get('revenueGrowth'))
    grossMargins_list.append(financialData_result.get('grossMargins'))
    ebitdaMargins_list.append(financialData_result.get('ebitdaMargins'))
    operatingMargins_list.append(financialData_result.get('operatingMargins'))
    profitMargins_list.append(financialData_result.get('profitMargins'))


    print("Symbol:", symbol_list)
    print("Market Cap:", marketCap_list)
    print("Type:", type_list)
    print("Long Name:", longName_list)
    print("Regular Market Change:", regularMarketChange_list)
    print("Regular Market Change Percent:", regularMarketChangePercent_list)
    print("Regular Market Time:", regularMarketTime_list)
    print("Regular Market Price:", regularMarketPrice_list)
    print("Regular Market Day High:", regularMarketDayHigh_list)
    print("Regular Market Day Range:", regularMarketDayRange_list)
    print("Regular Market Day Low:", regularMarketDayLow_list)
    print("Regular Market Volume:", regularMarketVolume_list)
    print("Regular Market Open:", regularMarketOpen_list)
    print("Average Daily Volume 3 Month:", averageDailyVolume3Month_list)
    print("Average Daily Volume 10 Day:", averageDailyVolume10Day_list)
    print("Fifty Two Week Range:", fiftyTwoWeekRange_list)
    print("Fifty Two Week Low:", fiftyTwoWeekLow_list)
    print("Fifty Two Week High:", fiftyTwoWeekHigh_list)
    print("Price Earnings:", priceEarnings_list)
    print("Earnings Per Share:", earningsPerShare_list)
    print("Logo URL:", logourl_list)


    print("Enterprise value:", enterpriseValue_list)
    print("Forward PE:", forwardPE_list)
    print("Float Shares:", floatShares_list)
    print("Shares Outstanding:", sharesOutstanding_list)
    print("Held Percent Insiders:", heldPercentInsiders_list)
    print("Held Percent Institutions:", heldPercentInstitutions_list)
    print("Beta:", beta_list)
    print("Implied Shares Outstanding:", impliedSharesOutstanding_list)
    print("Book Value:", bookValue_list)
    print("Price to Book:", priceToBook_list)
    print("Last Fiscal Year End:", lastFiscalYearEnd_list)
    print("Next Fiscal Year End:", nextFiscalYearEnd_list)
    print("Most Recent Quarter:", mostRecentQuarter_list)
    print("Earnings Quarterly Growth:", earningsQuarterlyGrowth_list)
    print("Net Income to Common:", netIncomeToCommon_list)
    print("Trailing EPS:", trailingEps_list)
    print("Forward EPS:", forwardEps_list)
    print("PEG Ratio:", pegRatio_list)
    print("Enterprise to Revenue:", enterpriseToRevenue_list)
    print("Enterprise to EBITDA:", enterpriseToEbitda_list)
    print("52 Week Change:", _52WeekChange_list)
    print("Last Dividend Value:", lastDividendValue_list)
    print("Last Dividend Date:", lastDividendDate_list)

    print("City:", city_list)
    print("State:", state_list)
    print("Country:", country_list)
    print("Website:", website_list)

    print("Target High Price:", targetHighPrice_list)
    print("Target Low Price:", targetLowPrice_list)
    print("Target Mean Price:", targetMeanPrice_list)
    print("Target Median Price:", targetMedianPrice_list)
    print("Recommendation Mean:", recommendationMean_list)
    print("Recommendation Key:", recommendationKey_list)
    print("Number Of Analyst Opinions:", numberOfAnalystOpinions_list)
    print("Total Cash:", totalCash_list)
    print("Total Cash Per Share:", totalCashPerShare_list)
    print("EBITDA:", ebitda_list)
    print("Total Debt:", totalDebt_list)
    print("Quick Ratio:", quickRatio_list)
    print("Current Ratio:", currentRatio_list)
    print("Total Revenue:", totalRevenue_list)
    print("Debt To Equity:", debtToEquity_list)
    print("Revenue Per Share:", revenuePerShare_list)
    print("Return On Assets:", returnOnAssets_list)
    print("Return On Equity:", returnOnEquity_list)
    print("Gross Profits:", grossProfits_list)
    print("Free Cashflow:", freeCashflow_list)
    print("Operating Cashflow:", operatingCashflow_list)
    print("Earnings Growth:", earningsGrowth_list)
    print("Revenue Growth:", revenueGrowth_list)
    print("Gross Margins:", grossMargins_list)
    print("EBITDA Margins:", ebitdaMargins_list)
    print("Operating Margins:", operatingMargins_list)
    print("Profit Margins:", profitMargins_list)



else:
    print(f"Request failed with status code {response.status_code}")
 