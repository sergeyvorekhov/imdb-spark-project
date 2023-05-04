from read_write import write
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.functions import expr

def task1(df, directory, amount= 5, is_write = True):
    temp_df = df.filter(expr("region == 'UA'"))
    if is_write:
        temp_df.select('title').show(amount)
        write(temp_df.select('title'), directory)
    else:
        temp_df.select('title', 'region').show(amount)


def task2(df, directory, amount= 15, is_write = True):
    temp_df = df.filter(expr("birthYear >= 1800 AND birthYear <= 1900"))
    if is_write:
        temp_df.select('primaryName').show(amount)
        write(temp_df.select('primaryName'), directory)
    else:
        temp_df.select('primaryName', 'birthYear').show(amount)

def task3(df, directory, amount= 15, is_write = True):
    temp_df = df.filter(expr("runtimeMinutes >= 120"))
    if is_write:
        temp_df.select('primaryTitle').show(amount)
        write(temp_df.select('primaryTitle'), directory)
    else:
        temp_df.select('primaryTitle', 'titleType', 'runtimeMinutes').show(amount)

def task4(df1, df2, df3, directory, amount= 100, is_write = True):

    """
        We have applied limitation because calculations are restricted on my computer.
    """
    df1 = df1.filter(expr("title IS NOT NULL AND titleId IS NOT NULL"))
    df1 = df1.limit(2000)
    df2 = df2.filter(expr("characters IS NOT NULL AND tconst IS NOT NULL AND LENGTH(characters) > 2"))
    df2 = df2.limit(2000)
    df3 = df3.filter(expr("primaryName IS NOT NULL AND nconst IS NOT NULL"))
    df3 = df3.limit(2000)
    temp_df = df1.join(df2, df1.titleId == df2.tconst, 'inner')
    temp_df = temp_df.join(df3, temp_df.nconst == df3.nconst, 'inner')
    if is_write:
        temp_df.select('primaryName', 'title', 'characters').distinct().show(amount, truncate=False)
        write(temp_df.select('primaryTitle'), directory)
    else:
        temp_df.select('primaryName', 'title', 'characters').distinct().show(amount, truncate=False)


def task5(df1, df2, directory, amount=10, is_write=False):

    """
        We have applied limitation because calculations are restricted on my computer.
    """

    df1.withColumn("titleId", f.trim(df1.titleId))
    df1 = df1.withColumn("region", f.regexp_replace(df1.region, "\\\\n", ""))
    df1 = df1.withColumn("region", f.regexp_replace(df1.region, "\\\\N", ""))
    df1 = df1.filter(expr("region != '' AND titleId IS NOT NULL"))
    df1 = df1.limit(200)
    df1.show()

    df2.withColumn("tconst", f.trim(df2.tconst))
    df2 = df2.limit(200)
    df2.show()

    temp_df = df1.join(df2, df1.titleId == df2.tconst, 'left')
#    temp_df.select('titleId', 'tconst', 'title', 'primaryTitle', 'isAdult', 'region').show(amount)
    temp_df = temp_df.groupBy('region').count().orderBy('count', ascending=False)
    temp_df = temp_df.limit(100)
    if is_write:
        write(temp_df, directory)
    else:
        temp_df.show(amount, truncate=False)


def task6(df1, df2, directory, amount=10, is_write=False):

    df1.withColumn("tconst", f.trim(df1.tconst))
    df1.withColumn("parentTconst", f.trim(df1.parentTconst))
    df1 = df1.groupBy('parentTconst').agg({'episodeNumber': 'count'}).orderBy('count(episodeNumber)', ascending=False)

    df2.withColumn("tconst", f.trim(df2.tconst))
    df2 = df2.filter(expr("tconst IS NOT NULL AND primaryTitle IS NOT NULL"))

    temp_df = df2.join(df1, df2.tconst == df1.parentTconst, 'left')
    temp_df = temp_df.select('tconst', 'primaryTitle', 'count(episodeNumber)').orderBy('count(episodeNumber)', ascending=False)
    temp_df = temp_df.limit(50)

    if is_write:
        write(temp_df, directory)
    else:
        temp_df.show(amount, truncate=False)

def task7(df1, df2, directory, amount=10, is_write=False):

    df1.withColumn("tconst", f.trim(df1.tconst))
    df1 = df1.filter(expr("averageRating > 0 AND numVotes > 0"))
#    df1 = df1.groupBy('parentTconst').agg({'episodeNumber': 'count'}).orderBy('count(episodeNumber)', ascending=False)

    df2.withColumn("tconst", f.trim(df2.tconst))
    df2.withColumn("decade", (f.floor(f.col("startYear") / 10) * 10).cast("int"))
    df2 = df2.filter(expr("tconst IS NOT NULL AND primaryTitle IS NOT NULL AND decade > 0"))
    df2 = df2.select('tconst', 'primaryTitle', 'decade')

    temp_df = df2.join(df1, df2.tconst == df1.parentTconst, 'left')
    temp_df = temp_df.select('tconst', 'primaryTitle', 'count(episodeNumber)').orderBy('count(episodeNumber)', ascending=False)
    temp_df = temp_df.limit(50)

    if is_write:
        write(temp_df, directory)
    else:
        temp_df.show(amount, truncate=False)
