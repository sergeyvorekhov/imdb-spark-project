from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark import SparkContext

from settings import *
from columns import schema_acas, schema_basics, schema_crew, schema_episode, schema_name_basics , schema_principals, schema_ratings
from read_write import read_all, read_tsv
from transformations import *

SparkContext().setLogLevel("OFF")

def switch(num, ss):

    temp_session = None
    second_df = None
    third_df = None

    if num == 1:
        temp_session = read_tsv(ss, TITLE_AKAS, schema_acas)
        task1(temp_session, FILE_1)
    elif num == 2:
        temp_session = read_tsv(ss, NAME_BASICS, schema_name_basics)
        task2(temp_session, FILE_2)
    elif num == 3:
        temp_session = read_tsv(ss, TITLE_BASICS, schema_basics)
        task3(temp_session, FILE_3)
    elif num == 4:
        temp_session = read_tsv(ss, TITLE_AKAS, schema_acas)
        second_df = read_tsv(ss, TITLE_PRICIPLES, schema_principals)
        third_df = read_tsv(ss, NAME_BASICS, schema_name_basics)
        task4(temp_session, second_df, third_df, FILE_4)
    elif num == 5:
        temp_session = read_tsv(ss, TITLE_AKAS, schema_acas)
        second_df = read_tsv(ss, TITLE_BASICS, schema_basics)
        task5(temp_session, second_df, FILE_5)
    elif num == 6:
        temp_session = read_tsv(ss, TITLE_EPISODE, schema_episode)
        second_df = read_tsv(ss, TITLE_BASICS, schema_basics)
        task6(temp_session, second_df, FILE_6)
    elif num == 7:
        spark = SparkSession.builder.appName("pyspark_window").getOrCreate()
        base_df = read_tsv(ss, TITLE_RATINGS, schema_ratings)
        second_df = read_tsv(spark, TITLE_BASICS, schema_basics)
        task7(base_df, second_df, FILE_7)
    elif num == 8:
        spark = SparkSession.builder.appName("pyspark_window").getOrCreate()
        base_df = read_tsv(ss, TITLE_RATINGS, schema_ratings)
        second_df = read_tsv(spark, TITLE_BASICS, schema_basics)
        task8(base_df, second_df, FILE_8)
    elif num == 9:
        read_all(ss, 5)

def main():
    """ Final project internals """
    spark_session = SparkSession.builder.getOrCreate()
    print("There are eight transformations.\n")
    print("1. Get all titles of series/movies etc. that are available in Ukrainian.")
    print("2. Get the list of people’s names, who were born in the 19th century.")
    print("3. Get titles of all movies that last more than 2 hours.")
    print("4. Get names of people, corresponding movies/series and characters they played in those films.")
    print("5. Get information about how many adult movies/series etc. there are per region. Get the top 100 of them from the region with the biggest count to the region with the smallest one.")
    print("6. Get information about how many episodes in each TV Series. Get the top 50 of them starting from the TV Series with the biggest quantity of episodes.")
    print("7. Get 10 titles of the most popular movies/series etc. by each decade.")
    print("8. Get 10 titles of the most popular movies/series etc. by each genre.\n")
    print("9. Just test the connection to DataFrames.\n")
    a = int(input("Please enter a number: "))
    switch(a, spark_session)

def testtask():
    ss = SparkSession.builder.getOrCreate()
    spark = SparkSession.builder.appName("pyspark_window").getOrCreate()
    base_df = read_tsv(ss, TITLE_RATINGS, schema_ratings)
    second_df = read_tsv(spark, TITLE_BASICS, schema_basics)
    task8(base_df, second_df, FILE_8)
#    read_all(ss, 500)

if __name__ == "__main__":
    main()
#    testtask()

