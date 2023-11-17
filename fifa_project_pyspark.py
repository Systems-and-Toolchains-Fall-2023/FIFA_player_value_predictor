#!/usr/bin/env python
# coding: utf-8

# Task 1

# In[1]:


import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.ml import Pipeline,Transformer
from pyspark.ml.feature import Imputer,StandardScaler,StringIndexer,OneHotEncoder, VectorAssembler

from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import os


# In[2]:


curr_dir = os.getcwd()

folder_path = os.path.join(curr_dir, "fifa_dataset")
files = os.listdir(folder_path)

male_files = [file for file in files if file.startswith('p') and file.endswith('.csv')]

appName = "FIFA Dataset Ingestion"
master = "local"


# In[3]:


get_ipython().system('hadoop fs -put players_15.csv /')
get_ipython().system('hadoop fs -put players_16.csv /')
get_ipython().system('hadoop fs -put players_17.csv /')
get_ipython().system('hadoop fs -put players_18.csv /')
get_ipython().system('hadoop fs -put players_19.csv /')
get_ipython().system('hadoop fs -put players_20.csv /')
get_ipython().system('hadoop fs -put players_21.csv /')
get_ipython().system('hadoop fs -put players_22.csv /')


# In[4]:


# Create Configuration object for Spark.
conf = spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SystemsToolChains") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Create Spark Context with the new configurations rather than relying on the default one
sc = SparkContext.getOrCreate(conf=conf)

# You need to create SQL Context to conduct some database operations like what we will see later.
sqlContext = SQLContext(sc)

# If you have SQL context, you create the session from the Spark Context
spark = sqlContext.sparkSession.builder.getOrCreate()


file_path = '/fifa_dataset/'
master_df = None
for csv_file in male_files:
#     file_path = os.path.join(folder_path, csv_file)
    file_path = '/' + csv_file
    current_df = spark.read.csv(file_path, header=True, inferSchema=True)
    year = f"20{csv_file.split('_')[-1].split('.')[0]}"
    current_df = current_df.withColumn("year", lit(year).cast("int"))
    if not master_df:
        master_df = current_df
    else:
        master_df = master_df.union(current_df)

master_df = master_df.withColumn("unique_id", monotonically_increasing_id())

master_df.printSchema()


# In[5]:


from pyspark.sql.functions import count, desc

def two_one(X):
  # only using players listed in the 2022 dataset
  df_2022 = master_df.filter(master_df['year'] == 2022)

  # filter for players with contract valid until 2023
  df_23 = df_2022.filter(df_2022['club_contract_valid_until'] == 2023)

  # find X clubs with highest number of players that fulfill conditions
  x_res = df_23.groupBy("club_name").agg(count("*").alias("contract_count"))
  x_res = x_res.orderBy(desc("contract_count"))
  x_res.show(X)

two_one(5)


# In[6]:


# y clubs w highest avg players 27+
from pyspark.sql.functions import col, avg, when,sum, countDistinct, desc, dense_rank
from pyspark.sql.window import Window

def two_two(Y):

  # create column denoting whether a players age is >= 27
  df_27 = master_df.withColumn("27+",when(col("age")>27,1).otherwise(0))

  # sum up
  y_res = df_27.groupBy("year","club_name").agg(sum("27+").alias("27+"))
  y_res = y_res.groupBy('club_name').agg(sum("27+") / countDistinct("year"))
  y_res = y_res.withColumnRenamed('(sum(27+) / count(year))', "avg_27+")
  y_res = y_res.orderBy(desc("avg_27+"))
  y_res = y_res.filter(col("club_name").isNotNull())

  # grab top Y clubs with the highest average age over 27
  window_spec = Window.partitionBy().orderBy(desc("avg_27+"))
  y_res = y_res.withColumn("rank",dense_rank().over(window_spec))
  top_y = y_res.filter(col("rank") <= Y)

  top_y.show()

two_two(5)


# In[7]:


# most frequent nation position by year
from pyspark.sql.functions import max, count


freq_np = master_df.groupBy("year", "nation_position").agg(count("*"))
freq_np = freq_np.orderBy("year",col("count(1)").desc())
freq_np = freq_np.filter(col("nation_position").isNotNull())
freq_np_res = freq_np.groupBy('year').agg(max("nation_position").alias("freq_pos"))
freq_np_res.show()


# Task 3

# In[8]:


# data cleaning & engineering
from pyspark.sql.functions import *
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer


# In[9]:


# drop columns with over 50% null values
null_percentage = [(col_name, (master_df.filter(col(col_name).isNull()).count() / master_df.count()) * 100.0)
                   for col_name in master_df.columns]

drop_50 = [col_name for col_name, percentage in null_percentage if percentage > 50.0]
master_df = master_df.drop(*drop_50)


# drop uninformative features
irrelevant_features = [
    'sofifa_id', 'player_url', 'short_name', 'long_name', 'player_positions', 'dob',
    'player_face_url', 'club_name', 'club_logo_url', 'club_flag_url', 'nation_team_id', 
    'nation_logo_url', 'nation_flag_url', 'club_jersey_number', 
    'club_loaned_from', 'release_clause_eur', 'player_tags', 
    'player_traits', 'real_face', 'nationality_id', 
    'nation_jersey_number', 'year'
] 
drop_feats = ['club_jersey_number','year','player_positions','nation_team_id','nation_jersey_number','club_logo_url','player_face_url','club_flag_url','dob','nation_logo_url','nation_flag_url','player_traits','player_tags','club_team_id','long_name','player_url','short_name']
master_df = master_df.drop(*irrelevant_features)


master_df.printSchema()


# In[10]:


from pyspark.sql.functions import regexp_replace, trim

master_df = master_df.withColumn("body_type", trim(regexp_replace("body_type", r'\(.*\)', '')))
master_df.select('body_type').show(15)


# In[11]:


from pyspark.sql.functions import udf, col, year
from pyspark.sql.types import IntegerType

def calculate_contract_length_spark(df):
    """
    Calculate the length of the contract for a Spark DataFrame.
    'club_joined' is a date from which the year will be extracted.
    'club_contract_valid_until' is an integer representing a year.
    """

    # Define a UDF to calculate contract length
    def contract_length_udf(joined_year, valid_until_year):
        if joined_year is not None and valid_until_year is not None:
            return valid_until_year - joined_year
        else:
            return None  # Return None for null or missing values

    # Register UDF
    contract_length_udf_spark = udf(contract_length_udf, IntegerType())

    # Extract year from 'club_joined' and cast 'club_contract_valid_until' to integer
    df = df.withColumn('joined_year', year(col('club_joined'))) \
           .withColumn('valid_until_year', col('club_contract_valid_until').cast(IntegerType()))

    # Apply UDF to calculate 'contract_length'
    df = df.withColumn('contract_length', contract_length_udf_spark(col('joined_year'), col('valid_until_year')))

    # Drop the intermediate columns
    df = df.drop('joined_year', 'valid_until_year', 'club_joined', 'club_contract_valid_until')


    return df

# Apply the function to your Spark DataFrame
master_df = calculate_contract_length_spark(master_df)

master_df.select('contract_length').show(15)


# In[12]:


def combine_skill_values(value):
    if isinstance(value, str):
        if '+' in value:
            parts = value.split('+')
            return int(parts[0]) + int(parts[1])
        elif '-' in value:
            parts = value.split('-')
            return int(parts[0]) - int(parts[1])
        else:
            return int(value)
    return value

skill_columns = [
    "ls", "st", "rs", "lw", "lf", "cf", "rf", "rw",
    "lam", "cam", "ram", "lm", "lcm", "cm", "rcm", "rm",
    "lwb", "ldm", "cdm", "rdm", "rwb", "lb", "lcb", "cb",
    "rcb", "rb", "gk"
]

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

map_skill = udf(combine_skill_values, IntegerType())

# Assuming master_df is your PySpark DataFrame
for col_name in skill_columns:
    master_df = master_df.withColumn(col_name, map_skill(master_df[col_name]))


# In[13]:


master_df = master_df.withColumn('mentality_composure', col('mentality_composure').cast(IntegerType()))
numerical_cols = [f.name for f in master_df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType, FloatType))]
categorical_cols = [f.name for f in master_df.schema.fields if isinstance(f.dataType, (StringType))]
numerical_cols.remove("overall")


# In[15]:


from pyspark.sql.functions import col, when
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, FeatureHasher
from pyspark.ml import Pipeline

def preprocess_and_split_spark_df(df, target_col_name, numerical_cols, categorical_cols, train_ratio=0.7, val_ratio=0.15):
    # Update categorical_cols to exclude 'nationality_name' and 'league_name'
    categorical_cols = [col for col in categorical_cols if col not in ['nationality_name', 'league_name']]

    # Splitting into features and target
    feature_df = df.drop(target_col_name)
    target_df = df.select("unique_id", target_col_name)

    # Impute missing values in numerical columns
    numerical_imputer = Imputer(inputCols=numerical_cols, outputCols=numerical_cols, strategy="mean")

    # normalize the numerical features
    numerical_assembler = VectorAssembler(inputCols=numerical_cols, outputCol="numerical_features")
    numerical_scaler = StandardScaler(inputCol="numerical_features", outputCol="scaled_numerical_features")

    # Impute missing values in categorical columns with the most frequent value 
    for col_name in categorical_cols:
        mode = feature_df.groupBy(col_name).count().orderBy('count', ascending=False).first()[0]
        feature_df = feature_df.withColumn(col_name, when(col(col_name).isNull(), mode).otherwise(col(col_name)))

    # StringIndexer and OneHotEncoder for categorical columns
    indexers = [StringIndexer(inputCol=col_name, outputCol=col_name + "_index", handleInvalid="keep") for col_name in categorical_cols]
    encoder = OneHotEncoder(inputCols=[col_name + "_index" for col_name in categorical_cols],
                            outputCols=[col_name + "_encoded" for col_name in categorical_cols])

    # Feature Hashers for 'nationality_name' and 'league_name'
    hasher_nationality = FeatureHasher(inputCols=["nationality_name"], outputCol="hashed_nationality", numFeatures=100)
    hasher_league = FeatureHasher(inputCols=["league_name"], outputCol="hashed_league", numFeatures=50)

    # Combine all preprocessors into a pipeline
    pipeline = Pipeline(stages=[numerical_imputer, numerical_assembler, numerical_scaler] + indexers + [encoder, hasher_nationality, hasher_league])
    transformed_feature_df = pipeline.fit(feature_df).transform(feature_df)

    # Join the transformed features with the target 
    joined_df = transformed_feature_df.join(target_df, "unique_id")

    # Apply VectorAssembler to the joined DataFrame
    assembler_inputs = [col + "_encoded" for col in categorical_cols] + ["scaled_numerical_features", "hashed_nationality", "hashed_league"]
    feature_assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="features")
    final_df = feature_assembler.transform(joined_df).select("features", target_col_name)

    
    final_df = final_df.drop('unique_id')

    # Train-Validation-Test Split
    train_df, val_df, test_df = final_df.randomSplit([0.7, 0.15, 0.15])

    return train_df, val_df, test_df

train_df, val_df, test_df = preprocess_and_split_spark_df(master_df, 'overall', numerical_cols, categorical_cols)


# In[16]:


# model 1: linear regression
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Define the Linear Regression model
lr = LinearRegression(featuresCol='features', labelCol='overall')

# Fit the model on the training dataset
lr_model = lr.fit(train_df)

# Transform the model on the training dataset to check its performance there
train_predictions = lr_model.transform(train_df)


# In[17]:


# Define an evaluator for regression with RMSE metric
evaluator = RegressionEvaluator(labelCol="overall", predictionCol="prediction", metricName="rmse")

# Evaluate the model on the training set
train_rmse = evaluator.evaluate(train_predictions)
print("Training Set - Root Mean Squared Error (RMSE):", train_rmse)
# Transform the model on the test dataset
test_predictions = lr_model.transform(test_df)

# Evaluate the model on the test set
test_rmse = evaluator.evaluate(test_predictions)
print("Test Set - Root Mean Squared Error (RMSE):", test_rmse)

# Optionally, you can also evaluate using MAE (Mean Absolute Error)
evaluator_mae = RegressionEvaluator(labelCol="overall", predictionCol="prediction", metricName="mae")
test_mae = evaluator_mae.evaluate(test_predictions)
print("Test Set - Mean Absolute Error (MAE):", test_mae)


# In[18]:


# hyperparameter tuning
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression

evaluator = RegressionEvaluator(labelCol="overall", predictionCol="prediction", metricName="rmse")
best_rmse = float("inf")
best_model = None
best_params = {}

for regParam in [0.001, 0.01, 0.1]:
    for maxIter in [100, 150]:
       
        lr = LinearRegression(featuresCol='features', labelCol='overall', regParam=regParam, maxIter=maxIter)

        # Fit the model
        model = lr.fit(train_df)

        # Perform evaluation on the validation set
        val_predictions = model.transform(val_df)
        rmse = evaluator.evaluate(val_predictions)

        # Log progress
        print(f"Params: regParam={regParam}, maxIter={maxIter}, RMSE={rmse}")

        # Update best model if current model is better
        if rmse < best_rmse:
            best_rmse = rmse
            best_model = model
            best_params = {'regParam': regParam, 'maxIter': maxIter}

# Log best parameters
print(f"Best Params: {best_params}, Best RMSE: {best_rmse}")


# In[19]:


# Define the best model with the optimal hyperparameters
best_model_lr = LinearRegression(featuresCol='features', labelCol='overall', regParam=0.01, maxIter=100)

# Fit the best model on the training dataset
best_model_fitted = best_model_lr.fit(train_df)

# Use the fitted model to make predictions on the test dataset
test_predictions = best_model_fitted.transform(test_df)

# Evaluate the model on the test set
test_rmse = evaluator.evaluate(test_predictions)
print("Test Set - Root Mean Squared Error (RMSE):", test_rmse)

# Optionally, evaluate using MAE (Mean Absolute Error)
evaluator_mae = RegressionEvaluator(labelCol="overall", predictionCol="prediction", metricName="mae")
test_mae = evaluator_mae.evaluate(test_predictions)
print("Test Set - Mean Absolute Error (MAE):", test_mae)


# In[20]:


# model 2 - random forest 

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Define the Random Forest Regression model
rf = RandomForestRegressor(featuresCol='features', labelCol='overall')

# Fit the model on the training dataset
rf_model = rf.fit(train_df)

# Transform the model on the training dataset to check its performance there
train_predictions_rf = rf_model.transform(train_df)

# Define an evaluator for regression with RMSE metric
evaluator_rf = RegressionEvaluator(labelCol="overall", predictionCol="prediction", metricName="rmse")

# Evaluate the model on the training set
train_rmse_rf = evaluator_rf.evaluate(train_predictions_rf)
print("Training Set - Root Mean Squared Error (RMSE):", train_rmse_rf)

# Transform the model on the test dataset
test_predictions_rf = rf_model.transform(test_df)

# Evaluate the model on the test set
test_rmse_rf = evaluator_rf.evaluate(test_predictions_rf)
print("Test Set - Root Mean Squared Error (RMSE):", test_rmse_rf)

# Optionally, evaluate using MAE (Mean Absolute Error)
evaluator_mae_rf = RegressionEvaluator(labelCol="overall", predictionCol="prediction", metricName="mae")
test_mae_rf = evaluator_mae_rf.evaluate(test_predictions_rf)
print("Test Set - Mean Absolute Error (MAE):", test_mae_rf)


# In[21]:


# Define the hyperparameter grid
numTreesList = [10, 20]
maxDepthList = [5, 10]

# Variables to store the best model's information
best_rmse_rf = float("inf")
best_model_rf = None
best_params_rf = {}

# Iterate over all combinations of hyperparameters
for numTrees in numTreesList:
    for maxDepth in maxDepthList:
        # Define the Random Forest model
        rf = RandomForestRegressor(featuresCol='features', labelCol='overall', numTrees=numTrees, maxDepth=maxDepth)

        # Fit the model on the training data
        model_rf = rf.fit(train_df)

        # Evaluate on the validation set
        val_predictions_rf = model_rf.transform(val_df)
        val_rmse_rf = evaluator_rf.evaluate(val_predictions_rf)

        # Log progress
        print(f"Params: numTrees={numTrees}, maxDepth={maxDepth}, Validation RMSE={val_rmse_rf}")

        # Update the best model if the current one is better
        if val_rmse_rf < best_rmse_rf:
            best_rmse_rf = val_rmse_rf
            best_model_rf = model_rf
            best_params_rf = {'numTrees': numTrees, 'maxDepth': maxDepth}

# Log the best parameters and RMSE
print(f"Best Parameters: {best_params_rf}, Best Validation RMSE: {best_rmse_rf}")


# In[22]:


# Evaluate the best model on the test set
test_predictions_rf = best_model_rf.transform(test_df)
test_rmse_rf = evaluator_rf.evaluate(test_predictions_rf)
print("Test Set - Best Model RMSE:", test_rmse_rf)

# Optionally, evaluate using MAE
evaluator_mae_rf = RegressionEvaluator(labelCol="overall", predictionCol="prediction", metricName="mae")
test_mae_rf = evaluator_mae_rf.evaluate(test_predictions_rf)
print("Test Set - Best Model MAE:", test_mae_rf)


# In[ ]:





# In[ ]:




