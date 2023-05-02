from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark import SparkContext

from settings import TITLE_AKAS, TITLE_BASICS, TITLE_CREW, TITLE_EPISODE, TITLE_PRICIPLES, TITLE_RATINGS, NAME_BASICS
from columns import schema_acas, schema_basics, schema_crew, schema_episode, schema_name_basics , schema_principals, schema_ratings

SparkContext().setLogLevel("WARN")


def main():

    spark_session = SparkSession.builder.getOrCreate()


    acas_df = spark_session.read.csv(TITLE_AKAS, header=True, nullValue="null", sep="\t", schema=schema_acas)
    acas_df.show(2)

    basics_df = spark_session.read.csv(TITLE_BASICS, header=True, sep="\t", schema=schema_basics)
    basics_df.show(2)

    crew_df = spark_session.read.csv(TITLE_CREW, header=True, sep="\t", schema=schema_crew)
    crew_df.show(2)

    episode_df = spark_session.read.csv(TITLE_EPISODE, header=True, sep="\t", schema=schema_episode)
    episode_df.show(2)

    principals_df = spark_session.read.csv(TITLE_PRICIPLES, header=True, sep="\t", schema=schema_principals)
    principals_df.show(2)

    ratings_df = spark_session.read.csv(TITLE_RATINGS, header=True, sep="\t", schema=schema_ratings)
    ratings_df.show(2)

    name_basics_df = spark_session.read.csv(NAME_BASICS, header=True, sep="\t", schema=schema_name_basics)
    name_basics_df.show(2)

""" """
if __name__ == "__main__":
    main()

