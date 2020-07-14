import re
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_date, udf, mean
from pyspark.sql.window import Window
from pyspark.sql.types import *


def time_formatter(x):
    """
    time_formatter convert PT__ formatted time in to minutes
    """
    # PTXXXXX => XXXXX
    val = x[2:-1]
    # check the length of the value
    if len(val) == 0:
        # If the length is 0, then the return 0
        return 0
    # if length of the value is not 0
    else:
        # there can be 3 possible patterns:
        # 1) Both hour and minute values exist
        regex_pt__h__m = re.compile('PT.*H.*M')
        # 2) Only hour value exist
        regex_pt__h = re.compile('PT.*H')
        # 3) Only minute value exist
        regex_pt__m = re.compile('PT.*M')

        # check for values with both hours and minutes
        if re.match(regex_pt__h__m, x):
            # PTXXHYYM => XX, YYM
            h, m = x[2:].split('H')
            # Convert XX to minutes + (YYM=>YY)
            return int(h) * 60 + int(m[:-1])
        # check for values with hour
        elif re.match(regex_pt__h, x):
            # Convert XX to minutes
            return int(val) * 60
        # check for values with minutes
        elif re.match(regex_pt__m, x):
            return int(val)


# register time_formatter as a user define function
udf_time_formatter = udf(time_formatter, IntegerType())


def difficulty_level_tagger(x, y):
    """
    This function use cookTime and prepTime in PT____ format as input then
    calculate the total cooking time using given formula return the difficulty level
    according to given conditions
    """
    # pt____ => minutes
    cooking_time = time_formatter(x)
    prep_time = time_formatter(y)

    total_cooking_time = cooking_time + prep_time

    if total_cooking_time < 30:
        return 'easy'
    elif 30 <= total_cooking_time <= 60:
        return 'medium'
    elif total_cooking_time > 60:
        return 'hard'


# register difficulty_level_tagger as a user define function
udf_difficulty_level_tagger = udf(difficulty_level_tagger, StringType())


# Create a spark session as spark
spark = SparkSession.builder.master("local").appName("HelloFresh Data Engineering Test").getOrCreate()

# create spark context as sc using spark (spark session)
sc = spark.sparkContext

# set log level to info
# sc.setLogLevel("INFO")
# create log object
log4jLogger = sc._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

sys_arg_error = 'You must specify 4 command line arguments according bellow order \n' \
                '1) input_path: full qualified path for input json file \n' \
                '2) output_path_1: path for save filtered data as a csv \n' \
                '3) output_path_2: path for save average cooking time as a csv \n' \
                '4) filter_ingredient: (Optional) default: Beef  '
try:
    # input_path: full qualified path for input json file
    input_path = sys.argv[1]
    # output_path_1: path for save filtered data as a csv
    output_path_1 = sys.argv[2]
    # output_path_2: path for save average cooking time as a csv
    output_path_2 = sys.argv[3]
except Exception as e:
    LOGGER.error(sys_arg_error)

try:
    # get 4th argument as the filter value if available
    filter_ingredient = sys.argv[4]
except:
    filter_ingredient = 'beef'

try:
    # Load json file as a spark DF
    df = spark.read.format("json").load(input_path). \
        select('cookTime', to_date(col('datePublished'), 'yyyy-MM-dd').alias('datePublished'),
               'description', 'ingredients', 'name', 'prepTime', 'recipeYield')

    # Get the latest record of recipes
    df2 = df.withColumn('rank', row_number().over(
        Window.partitionBy("name").orderBy(col("datePublished").desc())
    )).filter(col('rank') == 1)

    """
    Task 2.1
    extract only recipes that have beef as one of the ingredients
    """
    beef_df = df2.where(col('ingredients').contains(filter_ingredient))
    beef_df.coalesce(1).write.format("csv").option("header", "true").\
        save(output_path_1)

    """
    Task 2.2
    calculate average cooking time duration per difficulty level
    """
    # Convert PT__ formatted time in to more usable minutes
    # Calculate Difficulty level
    df2 = df2.select(udf_time_formatter('cookTime').alias('cookTimeMin'), 'cookTime',
                     'datePublished', 'description', 'ingredients', 'name', 'prepTime',
                     udf_time_formatter('prepTime').alias('prepTimeMin'),
                     'recipeYield', udf_difficulty_level_tagger('cookTime', 'prepTime').
                     alias('difficulty'))
    # calculate average cooking time duration per difficulty level
    df2.groupBy('difficulty').agg(mean('cookTimeMin').alias('avg_total_cooking_time')).\
        coalesce(1).write.format("csv").option("header", "true").\
        save(output_path_2)

except Exception as e:
    LOGGER.error(e)
#
