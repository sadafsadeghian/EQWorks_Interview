import os, sys
import numpy as np
import math
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct, least, mean, stddev
from pyspark.sql import functions as F

def main():
  spark = SparkSession.builder.appName("InterviewAnswers").getOrCreate()
  sc = spark.sparkContext
  sc.addPyFile("dependencies.zip")
  df = spark.read.format('csv').options(header='true', inferSchema='true').load('/tmp/data/DataSample.csv')
  # Question 1:
  poi_t = spark.read.format('csv').options(header='true', inferSchema='true').load('/tmp/data/POIList.csv')
  poi = poi_t.dropDuplicates([' Latitude', 'Longitude'])
  df1 = df.dropDuplicates([' TimeSt', 'Latitude', 'Longitude'])
  # df.count()
  poi = poi.select(col(" Latitude").alias("poi_lat"), col("Longitude").alias("poi_long"),col("POIID").alias("POIID"))

  # Question 2
  df_poi=df1.crossJoin(poi)


  def calculate(long, poi_long, lat, poi_lat):
    long_data=float(long)
    long_poi = float(poi_long)
    lat_data=float(lat)
    lat_poi = float(poi_lat)
    lat_diff= (lat_data-lat_poi)*(lat_data-lat_poi)
    long_diff= (long_data-long_poi)*(long_data-long_poi)

    return math.sqrt(lat_diff+long_diff)


  calculate_udf = udf(calculate)
  df_poi=df_poi.select("*", calculate_udf("Longitude","poi_long","Latitude","poi_lat").alias('Distance'))
  df_poi=df_poi.select("*",df_poi.Distance.cast('float').alias('Dist'))
  dd=df_poi.groupby('_ID','Latitude', 'Longitude', 'Country', 'Province', 'City', ' TimeSt').pivot('POIID').min("Dist")


  def get_list(a):
    df=a
    l=[]
    a=[]

    for r in df:
      a.append(r.asDict())

    for i in a:
      for k, v in i.items():
        l.append(v)

    return l


  POIs = poi.sort("POIID").select("POIID").distinct().collect()
  points = get_list(POIs)
  dd3 = dd.select("*", least(dd.POI1, dd.POI3, dd.POI4).alias('min'))


  def find_poi(poi_1, poi_3, poi_4, min_):
    poi1 = float(poi_1)
    poi3 = float(poi_3)
    poi4 = float(poi_4)
    minimum = float(min_)
    if (poi1 == minimum):
      point = "POI1"
    elif (poi3 == minimum):
      point = "POI3"
    elif (poi4 == minimum):
      point = "POI4"
    return point


  find_poi_udf = udf(find_poi)
  # column POINT of dataframe d_poi shows the nearest point of interest to the data point
  d_poi = dd3.select("*", find_poi_udf("POI1","POI3","POI4","min").alias('POINT'))

  # Question 3
  d_mean = d_poi.groupby("POINT").mean().sort("POINT")
  means = d_mean.select("avg(min)").collect()
  m = get_list(means)
  radius = d_poi.groupby("POINT").max().sort("POINT")
  counts = d_poi.groupby("POINT").count().sort("POINT")

  rr = radius.select("max(min)").collect()
  r = get_list(rr)

  stds = d_poi.groupby("POINT").agg(stddev("min")).sort("POINT")
  st = stds.select("stddev_samp(min)").collect()
  std = get_list(st)
  q4 = d_poi.join(stds, on="POINT", how="left")
  q4_2 = q4.join(d_mean, on="POINT", how="left")

  poi_lat = poi.sort("POIID").select("poi_lat").collect()
  poi_long = poi.sort("POIID").select("poi_long").collect()

  # plt.plot(poi_long[0],poi_lat[0],'s', markersize=8, color=colors[0])
  # plt.plot(poi_long[1],poi_lat[0],'.', markersize=4, color=colors[1])

  colors=['r', 'blue', 'g']
  marker=['+', '.', '<']
  z=[5,10,15]
  for i in range(len(std)):
    poi_plot=d_poi.where(col("POINT")==points[i])
    y = [val.Latitude for val in poi_plot.select('Latitude').collect()]
    x = [val.Longitude for val in poi_plot.select('Longitude').collect()]
    plt.plot(x, y, '.', markersize=1, color=colors[i])
    plt.plot(poi_long[i],poi_lat[i],'s', markersize=5, color=colors[i])
    plt.grid()
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(points[i])
    plt.savefig('poi_density_{}'.format(i))
    plt.cla()


  ## finding density
  requests= d_poi.groupby("POINT").count().sort("POINT")


  def find_area(radius):
    r = float(radius)
    area = 3.14159*r**2
    return area

  find_area_udf = udf(find_area)
  areas=radius.select("*", find_area_udf("max(min)").alias('area'))
  den= areas.join(counts, on = "POINT", how = "left")

  def find_density(area, count):
    a = float(area)
    c = float(count)
    den = c/a
    return den

  density_udf = udf(find_density)
  dense = den.select("*", density_udf("area", "count").alias("density"))
  density_poi = dense.select("density").collect()
  d = get_list(density_poi)
  densities = {}

  for i in range(len(d)):
    densities[points[i]] = d[i]
    #print(" Point:  ", points[i])
    #print(" has the density of: {}\n".format(d[i]))
  print('Request densities: ', densities)


if __name__== "__main__":
  main()