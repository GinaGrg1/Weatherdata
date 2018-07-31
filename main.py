import urllib
import json
import datetime as dt
import sys
import logging

import pandas as pd
from pyspark.sql import SparkSession

import config


def extractWeatherData(url):
    response_site = urllib.urlopen(url+apikey)
    try:
        utf8_data = response_site.read()
        logger.info("URL connected successfully.")
    except Exception as exn:
        logger.error("The error is {} : {}".format(exn.__class__.__name__, exn))

    json_obj = json.loads(utf8_data, encoding="utf-8")
    return json_obj['SiteRep']['DV']['Location']


def normaliseaspandasdf(json_obj, location_key=config.locationkeys()):
    return pd.io.json.json_normalize(json_obj, record_path=["Period", "Rep"], meta=location_key)


def extractweatherdatadaily(pandasdf):
    logger.info("Extracting the daily weather data.")
    renamed_df = pandasdf.rename(columns=config.dailydatacolumnsmapping())
    renamed_df["weather_date"] = pd.to_datetime(renamed_df["weather_date"], format="%Y-%m-%dZ")
    renamed_df = renamed_df.fillna(0)
    renamed_df.loc[:, config.dailydatacolconvert()] = renamed_df.loc[:, config.dailydatacolconvert()].apply(pd.to_numeric)
    return renamed_df


def formathourlydata(locationdata):
    for loc in locationdata:
        if isinstance(loc['Period'], dict):
            loc['Period'] = [loc['Period']]
            for period in loc['Period']:
                if isinstance(period['Rep'], dict):
                    period['Rep'] = [period['Rep']]
    return locationdata


def concat_time(r):
    return r["weather_date"] + dt.timedelta(seconds=r["minutes"]*60)


def extractweatherdatahourly(pandasdf):
    logger.info("Extracting the hourly weather data.")
    renamed_df_hr = pandasdf.rename(columns=config.hourlydatacolumnmapping())
    renamed_df_hr["weather_date"] = pd.to_datetime(renamed_df_hr["weather_date"], format="%Y-%m-%dZ")
    renamed_df_hr['pressure_tendency'] = renamed_df_hr['pressure_tendency'].fillna(value='NA')
    renamed_df_hr['wind_direction'] = renamed_df_hr['wind_direction'].fillna(value='NA')
    renamed_df_hr = renamed_df_hr.fillna(0)
    renamed_df_hr.loc[:, config.hourlydatacolconvert()] = renamed_df_hr.loc[:, config.hourlydatacolconvert()].apply(pd.to_numeric)
    renamed_df_hr["forecast_datetime"] = renamed_df_hr.apply(concat_time, axis=1)
    return renamed_df_hr


def converttosparkdataframe(pandasdf, spark):
    logger.info("Converting pandas dataframe to spark dataframe.")
    return spark.createDataFrame(pandasdf)


def main():
    sparksession = SparkSession.builder.appName("Extracting-weather-data").getOrCreate()
    urldaily, urlhourly = config.urls()

    if frequency.lower() == "daily":
        logger.info("Reading and extracting daily weather data.")
        dailylocationdata = extractWeatherData(urldaily)
        normalised_daily = normaliseaspandasdf(dailylocationdata)
        dailyweatherdata = extractweatherdatadaily(normalised_daily)
        #dailyweatherdata.to_csv("DailyWeatherData_{}.csv".format(dt.datetime.today().strftime('%Y-%m-%d')), header=True, index=False, encoding='utf-8')
        spark_df_daily = converttosparkdataframe(dailyweatherdata, sparksession)
        spark_df_daily.coalesce(1).write.csv(landing_path+"/temp")
        config.renamefile(landing_path+"/temp/part-*.csv", landing_path+"/DailyWeatherData_{}.csv".format(dt.datetime.today().strftime('%Y-%m-%d')))

    elif frequency.lower() == "hourly":
        logger.info("Reading and extracting hourly weather data.")
        hourlylocationdata = extractWeatherData(urlhourly)
        format_hourlydata = formathourlydata(hourlylocationdata)
        normalised_hourly = normaliseaspandasdf(format_hourlydata)
        hourlyweatherdata = extractweatherdatahourly(normalised_hourly)
        hourlyweatherdata.to_csv("HourlyWeatherData_{}.csv".format(dt.datetime.today().strftime('%Y-%m-%d_%H')), header=True, index=False, encoding='utf-8')
        #spark_df_hourly = converttosparkdataframe(hourlyweatherdata, sparksession)
        #spark_df_hourly.coalesce(1).write.csv(landing_path+"/temp")
        #config.renamefile(landing_path+"/temp/part-*.csv", landing_path+"/HourlyWeatherData_{}.csv".format(dt.datetime.today().strftime('%Y-%m-%d %H')))

    config.deletetemphdfsfolder(landing_path+"/temp")


if __name__ == '__main__':
    frequency = sys.argv[1].split("=")[1]
    landing_path = sys.arg[2].split("=")[1]
    apikey = sys.argv[3].split("=")[1]

    logging.basicConfig(
        filename="{}_{}-WeatherData_Log.log".format(dt.datetime.today().strftime('%Y-%m-%d_%H-%M-%S'), frequency.lower()),
        level=logging.DEBUG,
        format="%(levelname)s %(asctime)s - %(message)s",
        filemode='w')
    logger = logging.getLogger()

    main()
