# The code required to run the EQ interview questions

## To run the code, follow the instruction below:
1) Copy the Interview.py file into the 'data' folder in the host.
2) Run the docker file and attaching to the running process (docker exec command):
3) pip install matplotlib in the docker container
4) conda activate spark_job 
5) spark-submit --master spark://master:7077 /tmp/data/Interview.py




In this code questions 1 to 4a are answered. 

1) Line 19-24 are answering Question 1, removing the duplicated values.
2) Line 26 to 104 answer Question 2, finding the nearest POI to each data sample by first calculating the distance from each data sample to each POI, finding the minimum distance, and assigning the nearest POI to each data sample.
3) Line 105-176 answer both parts of question 3. First finding the mean and standard deviation for each POI data group, then calculating the density of each area as well as plotting their distribution with the circle showing the area for each POI. The figures for this section are also included in this repository with the names "poi_density_[POI_Name].png".
4) Line 177- 264 answer Question 4. First finding the outliers ( the 5% outside the range of 2*stddev around mean value ) for each POI group. Based on the new dataframe built, data for each POI group is then scaled to -10 to 10. The scaling is done considering the range of the data ( without outliers) and scaling it to -10 to 10. The scaled data to show the elimination of outliers are saved in fugires with the names "scaled_data_[POI_Name].png". 
