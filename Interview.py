import os, sys
import math
import copy
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct, least, mean, stddev
from pyspark.sql import functions as F

def main():
  
  ## Building Spark Session and Reading Data into DataFrames 
  spark = SparkSession.builder.appName("InterviewAnswers").getOrCreate()
  sc = spark.sparkContext
  # sc.addPyFile("dependencies.zip")
  df = spark.read.format('csv').options(header='true', inferSchema='true').load('/tmp/data/DataSample.csv')
  poi_t = spark.read.format('csv').options(header='true', inferSchema='true').load('/tmp/data/POIList.csv')
  
  # Question 1:
  # dropping he duplicated rows in both CSV files the Sample Data (duplicates in time stamp /Latitude / Longitude)  as well as the point of interest data ( duplicates in Latitude / Longitude)   
  poi = poi_t.dropDuplicates([' Latitude', 'Longitude'])
  df1 = df.dropDuplicates([' TimeSt', 'Latitude', 'Longitude'])
  poi = poi.select(col(" Latitude").alias("poi_lat"), col("Longitude").alias("poi_long"),col("POIID").alias("POIID"))

  
  # Question 2:
  
  def calculate(long, poi_long, lat, poi_lat):   
    """
    Args: 
    An object containing longitude of a data sample
    An object containing the longitude of the point of interest
    An object containing latitude of a data sample
    An object containing the latitude of the point of interest
    
    returns:
    the distance from the data sample to the point of interest
    """
    
    long_data = float(long)
    long_poi = float(poi_long)
    lat_data = float(lat)
    lat_poi = float(poi_lat)
    lat_diff = (lat_data-lat_poi)*(lat_data-lat_poi)
    long_diff = (long_data-long_poi)*(long_data-long_poi)

    return math.sqrt(lat_diff+long_diff)

  # Question 2
  # Cross joining the point of interest locations to the data samples to calculate
  # the distance between each sample point from the POI
  df_poi = df1.crossJoin(poi)
  calculate_udf = udf(calculate)
  df_poi = df_poi.select("*", calculate_udf("Longitude","poi_long","Latitude","poi_lat").alias('Distance'))
  df_poi = df_poi.select("*",df_poi.Distance.cast('float').alias('Dist'))
  dd=df_poi.groupby('_ID','Latitude', 'Longitude', 'Country', 'Province', 'City', ' TimeSt').pivot('POIID').min("Dist")

  def get_list(a):
    """
    Args:
    A Row Object
    
    Returns:
    A list containing the values in the row
    """
    df = a
    l = []
    a = []
    for r in df:
      a.append(r.asDict())
    for i in a:
      for k, v in i.items():
        l.append(v)

    return l


  POIs = poi.sort("POIID").select("POIID").distinct().collect()
  points = get_list(POIs)
  
  dd3=dd.select("*", least(*points).alias('min'))
  def find_index(*args):
    point = len(args)
    minimum=float(args[-1])
    for i in range(len(args)-1):
      if ((float(args[i]))==minimum):
        point= i
    return point
  def find_poi (index, poi_list):
    a=int(index)
    return poi_list[a]
  
  def udf_find_poi(label_list):
    return udf(lambda l: find_poi(l, poi_list))
  poi_list=points

  aa=copy.deepcopy(points)
  aa.append("min")

  find_index_udf = udf(find_index)
  d_poi_temp=dd3.select("*", find_index_udf(*aa).alias('Index'))
  
  # column POINT of dataframe d_poi shows the nearest point of interest to the data point
  d_poi=d_poi_temp.select("*", udf_find_poi(poi_list)(col("index")).alias("POINT"))

  ##Question 3 - Part 1:
  
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
  # q4_2 = q4.join(d_mean, on="POINT", how="left")

  poi_lat = get_list(poi.sort("POIID").select("poi_lat").collect())
  poi_long= get_list(poi.sort("POIID").select("poi_long").collect())

  colors=['r', 'blue', 'g']
  # marker=['+', '.', '<']
  z = [5, 10, 15]
  theta = range(1, int(20000 * math.pi), 1)
  for i in range(len(std)):
    poi_plot=d_poi.where(col("POINT")==points[i])
    y = [val.Latitude for val in poi_plot.select('Latitude').collect()]
    x = [val.Longitude for val in poi_plot.select('Longitude').collect()]
    plt.plot(x, y, '.', markersize=1, color=colors[i])
    plt.plot(poi_long[i],poi_lat[i],'s', markersize=5, color=colors[i])
    a = [math.cos(x/20000)*r[i]**2+poi_long[i] for x in theta]
    b = [math.sin(x/20000)*r[i]**2+poi_lat[i] for x in theta]
    c = [-1*math.sin(x/20000)*r[i]**2+poi_lat[i] for x in theta]
    plt.plot(a, b, color=colors[i])
    plt.plot(a, c, color=colors[i])
    plt.grid()
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title(points[i])
    plt.savefig('/tmp/data/poi_density_{}'.format(points[i]))
    plt.cla()

  ## Question 3 - Part 2
  ## finding density
  requests = d_poi.groupby("POINT").count().sort("POINT")

  def find_area(radius):
    r = float(radius)
    area = 3.14159 * r ** 2
    return area

  find_area_udf = udf(find_area)
  areas = radius.select("*", find_area_udf("max(min)").alias('area'))
  den = areas.join(counts, on="POINT", how="left")

  def find_density(area, count):
    a = float(area)
    c = float(count)
    den = c / a
    return den

  density_udf = udf(find_density)
  dense = den.select("*", density_udf("area", "count").alias("density"))
  density_poi = dense.select("density").collect()
  d = get_list(density_poi)
  densities = {}

  for i in range(len(d)):
    densities[points[i]] = d[i]

  print('Request densities: ', densities)

  ## Question 4
  q_outlier = stds.join(d_mean, on="POINT", how="left")
  q_outlier = q_outlier.select(col("POINT").alias("POINT"), col("stddev_samp(min)").alias("stddev"),
                               col("avg(min)").alias("mean"))

  q4 = d_poi.join(q_outlier, on="POINT", how="left")

  poi = poi.select(col("poi_lat").alias("poi_lat"), col("poi_long").alias("poi_long"), col("POIID").alias("POINT"))
  q4 = q4.join(poi, on="POINT", how="left")

  q4 = q4.select(col("POINT").alias("POINT"), col("_ID").alias("ID"), col("poi_lat").alias("poi_lat"),
                 col("poi_long").alias("poi_long"), col("Latitude").alias("Latitude"),
                 col("Longitude").alias("Longitude"), col("Country").alias("Country"),
                 col("Province").alias("Provice"), col("City").alias("City"),
                 col(" TimeSt").alias("TimeSt"), col("stddev").alias("stddev"),
                 col("min").alias("distance"), col("mean").alias("mean"))

  def outlier(dist, stddev, mean):
    d = float(dist)
    std = float(stddev)
    m = float(mean)
    if (d <= (mean + 2 * std) and d >= (mean - 2 * std)):
      a = 0
    else:
      a = 1
    return a

  outlier_udf = udf(outlier)
  q4_outlier = q4.select("*", outlier_udf("distance", "stddev", "mean").alias("outlier"))

  d_non_outlier = q4_outlier.where(col("outlier") == 0)

  print(
    "There were {0} datas before removing outliers "
    "and after removing outliers there are: {1} datas".format(q4.count(), d_non_outlier.count()))

  poi_min = d_non_outlier.groupby("POINT").min().sort("POINT")
  poi_min = poi_min.select(col("POINT"), col("min(Latitude)"), col("min(Longitude)"))

  poi_max = d_non_outlier.groupby("POINT").max().sort("POINT")
  poi_max = poi_max.select(col("POINT"), col("max(Latitude)"), col("max(Longitude)"))

  poi_scale_step1 = poi_min.join(poi_max, on="POINT", how="left")
  poi_scale_step2 = d_non_outlier.join(poi_scale_step1, on="POINT", how="left")

  def scaling_latitude(lat, min_lat, max_lat):
    lat = float(lat)
    min_lat = float(min_lat)
    max_lat = float(max_lat)
    scaled = (lat - min_lat) / (max_lat - min_lat) * (20) - 10
    return scaled

  def scaling_longitude(long, min_long, max_long):
    long = float(long)
    min_long = float(min_long)
    max_long = float(max_long)
    scaled = (long - min_long) / (max_long - min_long) * (20) - 10
    return scaled

  scaling_lat_udf = udf(scaling_latitude)
  scaled_q4 = poi_scale_step2.select("*",
                                     scaling_lat_udf("Latitude", "min(Latitude)", "max(Latitude)").alias('lat_scaled'))
  scaled_q4 = scaled_q4.select("*",
                               scaling_lat_udf("poi_lat", "min(Latitude)", "max(Latitude)").alias("poi_lat_scaled"))

  scaling_long_udf = udf(scaling_longitude)
  scaled_q4 = scaled_q4.select("*",
                               scaling_long_udf("Longitude", "min(Longitude)", "max(Longitude)").alias('long_scaled'))
  scaled_q4 = scaled_q4.select("*",
                               scaling_long_udf("poi_long", "min(Longitude)",
                                                "max(Longitude)").alias("poi_long_scaled"))

  # plot the final scaled dataframe
  plt.cla()
  for i in range(len(points)):
    poi_plot_scaled = scaled_q4.where(col("POINT") == points[i])
    y = get_list(poi_plot_scaled.select('lat_scaled').collect())
    x = get_list(poi_plot_scaled.select('long_scaled').collect())
    plt.plot(x, y, '.', markersize=1, color=colors[i])
    plt.xlabel("Scaled Longitude")
    plt.ylabel("Scaled Latitude")
    plt.axis("image")
    plt.tick_params(axis='both', left='off', top='off', right='off', bottom='off', labelleft='off', labeltop='off',
                    labelright='off', labelbottom='off')
    plt.title(points[i])
    plt.savefig("/tmp/data/scaled_data_{}.png".format(points[i]))
    plt.cla()

  print('Request POI densities: ', densities)

if __name__== "__main__":
  main()
