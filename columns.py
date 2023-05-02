from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType

# Specify schema title.akas.tsv.gz
schema_acas = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True),
    StructField("attributes", StringType(), True),
    StructField("isOriginalTitle", BooleanType(), True)
    ])

# Specify schema title.basics.tsv.gz
schema_basics = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", IntegerType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", BooleanType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", StringType(), True)
    ])

# Specify schema title.crew.tsv.gz
schema_crew = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True)
    ])

# Specify schema title.episode.tsv.gz
schema_episode = StructType([
    StructField("tconst", StringType(), True),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", IntegerType(), True),
    StructField("episodeNumber", IntegerType(), True)
    ])

# Specify schema title.principals.tsv.gz
schema_principals = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True)
    ])

# Specify schema title.ratings.tsv.gz
schema_ratings = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", StringType(), True),
    StructField("numVotes", IntegerType(), True)
    ])

# Specify schema name.basics.tsv.gz
schema_name_basics = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", IntegerType(), True),
    StructField("deathYear", IntegerType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True)
    ])
