import json
import requests
import logging
import pymysql
import os


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# API URL
url = "https://brapi.dev/api/quote/list?token=fUNoicPSZTzstUdqw1uVeB"

# Send a GET request to the API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
else:
    logger.error("Failed to retrieve data: %s", response.status_code)
    # Depending on your use case, you might want to exit or handle this differently
    raise Exception("Failed to retrieve data from the API")

# Access the 'stocks' part of the data
stocks_data = data['stocks']

# Filter to keep only components where 'type' is 'stock' and not ending with 'F'
filtered_stocks = [item for item in stocks_data if item.get('type') == 'stock' and not item.get('stock', '').endswith('F')]

# Initialize empty listsa
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

def format_date(date_string):
    """Safely formats a date string from 'YYYY-MM-DD' to 'DD-MM-YYYY'. Returns None if input is None."""
    if date_string:
        try:
            return '-'.join(date_string[:10].split('-')[::-1])
        except Exception as e:
            logger.error(f"Error formatting date: {date_string}. Error: {e}")
            return None
    else:
        return None

# Iterate over unique tickers
for stock in filtered_stocks:
    ticker = stock.get('stock')
    url = f"https://brapi.dev/api/quote/{ticker}"
    params = {
        'range': '1d',
        'interval': '1d',
        'modules': 'balanceSheetHistory,defaultKeyStatistics,summaryProfile,financialData',
        'token': 'fUNoicPSZTzstUdqw1uVeB',
    }
    #url = f"https://brapi.dev/api/quote/{ticker}?range=1d&interval=1d&fundamental=true&dividends=true&modules=balanceSheetHistory,defaultKeyStatistics,summaryProfile,financialData&token=fUNoicPSZTzstUdqw1uVeB"
    response = requests.get(url, params=params)    
    if response.status_code == 200:
        results = response.json()
        first_result = results.get('results')[0]  # Adjust according to the actual JSON structure
        keyStatistics_result = results.get('results')[0].get('defaultKeyStatistics', {}) # Adjust according to the actual JSON structure
        summaryProfile_result = results.get('results')[0].get('summaryProfile', {})  # Adjust according to the actual JSON structure
        financialData_result = results.get('results')[0].get('financialData', {})  # Adjust according to the actual JSON structure
        
        # Extract and append the data to respective lists
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
        lastFiscalYearEnd = format_date(first_result.get('lastFiscalYearEnd'))
        if lastFiscalYearEnd:
            lastFiscalYearEnd_list.append(lastFiscalYearEnd)
        else:
            lastFiscalYearEnd_list.append("N/A")  # or whatever default value you prefer

        nextFiscalYearEnd = format_date(first_result.get('nextFiscalYearEnd'))
        #nextFiscalYearEnd_list.append('-'.join(keyStatistics_result.get('nextFiscalYearEnd')[:10].split('-')[::-1]))
        if nextFiscalYearEnd:
            nextFiscalYearEnd_list.append(nextFiscalYearEnd)
        else:
            nextFiscalYearEnd_list.append("N/A")  # or whatever default value you prefer
        #mostRecentQuarter_list.append('-'.join(keyStatistics_result.get('mostRecentQuarter')[:10].split('-')[::-1]))
        mostRecentQuarter = format_date(first_result.get('mostRecentQuarter'))
        if mostRecentQuarter:
            mostRecentQuarter_list.append(mostRecentQuarter)
        else:
            mostRecentQuarter_list.append("N/A")  # or whatever default value you prefer

        nextFiscalYearEnd = format_date(first_result.get('lastFiscalYearEnd'))
        earningsQuarterlyGrowth_list.append(keyStatistics_result.get('earningsQuarterlyGrowth'))
        netIncomeToCommon_list.append(keyStatistics_result.get('netIncomeToCommon'))
        trailingEps_list.append(keyStatistics_result.get('trailingEps'))
        forwardEps_list.append(keyStatistics_result.get('forwardEps'))
        pegRatio_list.append(keyStatistics_result.get('pegRatio'))
        enterpriseToRevenue_list.append(keyStatistics_result.get('enterpriseToRevenue'))
        enterpriseToEbitda_list.append(keyStatistics_result.get('enterpriseToEbitda'))
        _52WeekChange_list.append(keyStatistics_result.get('52WeekChange'))
        lastDividendValue_list.append(keyStatistics_result.get('lastDividendValue'))
        #lastDividendDate_list.append('-'.join(keyStatistics_result.get('lastDividendDate')[:10].split('-')[::-1]))
        lastDividendDate = format_date(first_result.get('lastDividendDate'))
        if mostRecentQuarter:
            lastDividendDate_list.append(lastDividendDate)
        else:
            lastDividendDate_list.append("N/A")  # or whatever default value you prefer


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
    else:
        logger.error("Failed to retrieve data for ticker: %s, Status code: %s", ticker, response.status_code)
        # Handle or log the failure as per your requirement

# Database connection details
host = 'brapi-api-db.cx0ko2c0yzso.us-east-2.rds.amazonaws.com'
user = 'admin'
password = 'G78u75s61T91!'
db_name = 'brapi_API_DB'

# Initialize connection to None
connection = None

try:
    connection = pymysql.connect(host=host, user=user, password=password, db=db_name, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
    logger.info("Connected to the database successfully")

    with connection.cursor() as cursor:
        cursor.execute("SET SQL_SAFE_UPDATES = 0;")
        cursor.execute("DELETE FROM brapi_API_data_daily;")
        cursor.execute("SET SQL_SAFE_UPDATES = 1;")

        for i in range(len(symbol_list)):
            sql_query = """
            INSERT INTO stock_indicators (
                symbol, marketCap, type, longName, regularMarketChange, regularMarketChangePercent,
                regularMarketTime, regularMarketPrice, regularMarketDayHigh, regularMarketDayRange,
                regularMarketDayLow, regularMarketVolume, regularMarketOpen, averageDailyVolume3Month,
                averageDailyVolume10Day, fiftyTwoWeekRange, fiftyTwoWeekLow, fiftyTwoWeekHigh,
                priceEarnings, earningsPerShare, logourl, enterpriseValue, forwardPE, floatShares,
                sharesOutstanding, heldPercentInsiders, heldPercentInstitutions, beta, 
                impliedSharesOutstanding, bookValue, priceToBook, lastFiscalYearEnd, nextFiscalYearEnd,
                mostRecentQuarter, earningsQuarterlyGrowth, netIncomeToCommon, trailingEps, forwardEps,
                pegRatio, enterpriseToRevenue, enterpriseToEbitda, _52WeekChange, lastDividendValue,
                lastDividendDate, city, state, country, website, targetHighPrice, targetLowPrice,
                targetMeanPrice, targetMedianPrice, recommendationMean, recommendationKey, numberOfAnalystOpinions,
                totalCash, totalCashPerShare, ebitda, totalDebt, quickRatio, currentRatio,
                totalRevenue, debtToEquity, revenuePerShare, returnOnAssets, returnOnEquity, grossProfits,
                freeCashflow, operatingCashflow, earningsGrowth, revenueGrowth, grossMargins, ebitdaMargins,
                operatingMargins, profitMargins
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql_query, (
                symbol_list[i], marketCap_list[i], type_list[i], longName_list[i], regularMarketChange_list[i], regularMarketChangePercent_list[i],
                regularMarketTime_list[i], regularMarketPrice_list[i], regularMarketDayHigh_list[i], regularMarketDayRange_list[i],
                regularMarketDayLow_list[i], regularMarketVolume_list[i], regularMarketOpen_list[i], averageDailyVolume3Month_list[i],
                averageDailyVolume10Day_list[i], fiftyTwoWeekRange_list[i], fiftyTwoWeekLow_list[i], fiftyTwoWeekHigh_list[i],
                priceEarnings_list[i], earningsPerShare_list[i], logourl_list[i], enterpriseValue_list[i], forwardPE_list[i], floatShares_list[i],
                sharesOutstanding_list[i], heldPercentInsiders_list[i], heldPercentInstitutions_list[i], beta_list[i], 
                impliedSharesOutstanding_list[i], bookValue_list[i], priceToBook_list[i], lastFiscalYearEnd_list[i], nextFiscalYearEnd_list[i],
                mostRecentQuarter_list[i], earningsQuarterlyGrowth_list[i], netIncomeToCommon_list[i], trailingEps_list[i], forwardEps_list[i],
                pegRatio_list[i], enterpriseToRevenue_list[i], enterpriseToEbitda_list[i], _52WeekChange_list[i], lastDividendValue_list[i],
                lastDividendDate_list[i], city_list[i], state_list[i], country_list[i], website_list[i], targetHighPrice_list[i], targetLowPrice_list[i],
                targetMeanPrice_list[i], targetMedianPrice_list[i], recommendationMean_list[i], recommendationKey_list[i], numberOfAnalystOpinions_list[i],
                totalCash_list[i], totalCashPerShare_list[i], ebitda_list[i], totalDebt_list[i], quickRatio_list[i], currentRatio_list[i],
                totalRevenue_list[i], debtToEquity_list[i], revenuePerShare_list[i], returnOnAssets_list[i], returnOnEquity_list[i], grossProfits_list[i],
                freeCashflow_list[i], operatingCashflow_list[i], earningsGrowth_list[i], revenueGrowth_list[i], grossMargins_list[i], ebitdaMargins_list[i],
                operatingMargins_list[i], profitMargins_list[i]
            ))

        connection.commit()
        logger.info("Data inserted successfully")

except Exception as e:
    logger.error("Error connecting to or interacting with the database: %s", e)
finally:
    if connection:
        connection.close()
        logger.info("Database connection closed")
