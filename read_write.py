from settings import TITLE_AKAS, TITLE_BASICS, TITLE_CREW, TITLE_EPISODE, TITLE_PRICIPLES, TITLE_RATINGS, NAME_BASICS
from columns import schema_acas, schema_basics, schema_crew, schema_episode, schema_name_basics , schema_principals, schema_ratings
from pyspark.sql.functions import expr

def read_all(ss, amount):

    acas_df = ss.read.csv(TITLE_AKAS, header=True, nullValue="null", sep="\t", schema=schema_acas)
    acas_df.show(amount)

    basics_df = ss.read.csv(TITLE_BASICS, header=True, sep="\t", schema=schema_basics)
    basics_df.show(amount)

    crew_df = ss.read.csv(TITLE_CREW, header=True, sep="\t", schema=schema_crew)
    crew_df.show(amount)

    episode_df = ss.read.csv(TITLE_EPISODE, header=True, sep="\t", schema=schema_episode)
    episode_df.show(amount)

    principals_df = ss.read.csv(TITLE_PRICIPLES, header=True, sep="\t", schema=schema_principals)
    principals_df.show(amount)

    ratings_df = ss.read.csv(TITLE_RATINGS, header=True, sep="\t", schema=schema_ratings)
    ratings_df.show(amount)

    name_basics_df = ss.read.csv(NAME_BASICS, header=True, sep="\t", schema=schema_name_basics)
    name_basics_df.show(amount)


def read_tsv(ss, path= "", temp_schema= ""):
    try:
      temp_df = ss.read.csv(path, header=True, nullValue="null", sep="\t", schema=temp_schema)
      return temp_df
    except ValueError:
      print("Something went wrong when reading to the file " + path)
      print(ValueError)
      return False


def write(df, directory_to_write):
    df.write.csv(directory_to_write, header=True)
    return
