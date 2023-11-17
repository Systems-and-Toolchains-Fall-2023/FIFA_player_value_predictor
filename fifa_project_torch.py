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


# Task 3

# In[5]:


# data cleaning & engineering
from pyspark.sql.functions import *
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer


# In[6]:


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


# In[7]:


from pyspark.sql.functions import regexp_replace, trim

master_df = master_df.withColumn("body_type", trim(regexp_replace("body_type", r'\(.*\)', '')))
master_df.select('body_type').show(15)


# In[8]:


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


# In[9]:


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


# In[10]:


master_df = master_df.withColumn('mentality_composure', col('mentality_composure').cast(IntegerType()))
numerical_cols = [f.name for f in master_df.schema.fields if isinstance(f.dataType, (IntegerType, DoubleType, FloatType))]
categorical_cols = [f.name for f in master_df.schema.fields if isinstance(f.dataType, (StringType))]
numerical_cols.remove("overall")


# In[11]:


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


# In[12]:


# Converting the Spark DF to Tensors (both PyTorch and TensorFlow)
train_pandas_df = train_df.toPandas()
val_pandas_df = val_df.toPandas()
test_pandas_df = test_df.toPandas()


# In[13]:


spark.stop()


# In[14]:


import torch

# convert a DataFrame to tensors for PyTorch
def convert_to_tensors(df):
    features = torch.tensor(df['features'].apply(lambda x: x.toArray()).tolist()).float()
    targets = torch.tensor(df['overall'].values).float()
    return features, targets


train_features_tensor, train_targets_tensor = convert_to_tensors(train_pandas_df)
val_features_tensor, val_targets_tensor = convert_to_tensors(val_pandas_df)
test_features_tensor, test_targets_tensor = convert_to_tensors(test_pandas_df)


# In[15]:


import tensorflow as tf
import numpy as np

# convert a DataFrame to tensors for TensorFlow
def convert_to_tf_tensors(df):
    features = np.array(df['features'].apply(lambda x: x.toArray()).tolist())
    targets = df['overall'].values
    return tf.convert_to_tensor(features, dtype=tf.float32), tf.convert_to_tensor(targets, dtype=tf.float32)


train_features_tensor_tf, train_targets_tensor_tf = convert_to_tf_tensors(train_pandas_df)
val_features_tensor_tf, val_targets_tensor_tf = convert_to_tf_tensors(val_pandas_df)
test_features_tensor_tf, test_targets_tensor_tf = convert_to_tf_tensors(test_pandas_df)


# In[16]:


import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import TensorDataset, DataLoader

class VanillaNet(nn.Module):
    def __init__(self):
        super(VanillaNet, self).__init__()
        self.fc1 = nn.Linear(273, 128)  # 273 input features
        self.fc2 = nn.Linear(128, 1)    # Regression task

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = self.fc2(x)
        return x


# In[17]:


# Creating datasets and loaders
train_dataset = TensorDataset(train_features_tensor, train_targets_tensor)
val_dataset = TensorDataset(val_features_tensor, val_targets_tensor)
test_dataset = TensorDataset(test_features_tensor, test_targets_tensor)

train_loader = DataLoader(dataset=train_dataset, batch_size=64, shuffle=True)
val_loader = DataLoader(dataset=val_dataset, batch_size=64, shuffle=False)

# Initialize model, loss, and optimizer
model = VanillaNet()
criterion = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Training loop
for epoch in range(100):  # 100 epochs
    model.train()
    total_train_loss = 0
    for inputs, targets in train_loader:
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, targets.view(-1, 1))
        loss.backward()
        optimizer.step()
        total_train_loss += loss.item()

    avg_train_loss = total_train_loss / len(train_loader)

    # Validation phase
    model.eval()
    total_val_loss = 0
    with torch.no_grad():
        for inputs, targets in val_loader:
            outputs = model(inputs)
            loss = criterion(outputs, targets.view(-1, 1))
            total_val_loss += loss.item()

    avg_val_loss = total_val_loss / len(val_loader)

    # Logging training and validation loss
    print(f'Epoch [{epoch+1}/{100}], Train Loss: {avg_train_loss:.4f}, Val Loss: {avg_val_loss:.4f}')

# Evaluate on test set
model.eval()
total_test_loss = 0
with torch.no_grad():
    for inputs, targets in DataLoader(dataset=test_dataset, batch_size=64, shuffle=False):
        outputs = model(inputs)
        loss = criterion(outputs, targets.view(-1, 1))
        total_test_loss += loss.item()

avg_test_loss = total_test_loss / len(DataLoader(dataset=test_dataset, batch_size=64))
test_rmse = torch.sqrt(torch.tensor(avg_test_loss))
print(f'Test RMSE: {test_rmse.item()}')


# In[18]:


best_rmse = float('inf')
best_lr = None
best_optimizer_type = None

for lr in [0.001, 0.01, 0.1]:
    for optimizer_type in [optim.Adam, optim.SGD]:
        model = VanillaNet()
        optimizer = optimizer_type(model.parameters(), lr=lr)

        print(f"Training with LR: {lr} and Optimizer: {'Adam' if optimizer_type == optim.Adam else 'SGD'}")
        for epoch in range(100):  # 100 epochs for tuning
            model.train()
            for inputs, targets in train_loader:
                optimizer.zero_grad()
                outputs = model(inputs)
                loss = criterion(outputs, targets.view(-1, 1))
                loss.backward()
                optimizer.step()

        # Evaluate on validation set
        model.eval()
        with torch.no_grad():
            val_outputs = model(val_features_tensor)
            val_loss = torch.sqrt(criterion(val_outputs, val_targets_tensor.view(-1, 1)))
            val_rmse = val_loss.item()
        print(f"Validation RMSE: {val_rmse}")

        if val_rmse < best_rmse:
            best_rmse = val_rmse
            best_lr = lr
            best_optimizer_type = optimizer_type
            best_model_state = model.state_dict()

print(f'Best Learning Rate: {best_lr}, Best Optimizer: {best_optimizer_type.__name__}, Best RMSE: {best_rmse}')


# In[19]:


tuned_model = VanillaNet()
tuned_model.load_state_dict(best_model_state)

# Evaluate the best model on test set
tuned_model.eval()
with torch.no_grad():
    test_outputs = tuned_model(test_features_tensor)
    test_loss = torch.sqrt(criterion(test_outputs, test_targets_tensor.view(-1, 1)))
    print(f'Test RMSE with Best Model: {test_loss.item()}')


# After tuning our vanilla feedforward neural network to a learning rate of 0.001 and the Adam optimizer, we again found a very minimal RMSE of 0.727 which became 0.7311 on the test set. The vanilla feedforward neural net does about as well as the SparkML random forest model.

# In[20]:


# model 2: MLP


# In[21]:


class MLP(nn.Module):
    def __init__(self):
        super(MLP, self).__init__()
        self.fc1 = nn.Linear(273, 128)
        self.fc2 = nn.Linear(128, 64)
        self.fc3 = nn.Linear(64, 32)
        self.fc4 = nn.Linear(32, 1)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = torch.relu(self.fc2(x))
        x = torch.relu(self.fc3(x))
        x = self.fc4(x)
        return x


# In[22]:


# Initialize model, loss, and optimizer
model = MLP()
criterion = nn.MSELoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

# Training loop
for epoch in range(100):  # 100 epochs
    model.train()
    total_train_loss = 0
    for inputs, targets in train_loader:
        optimizer.zero_grad()
        outputs = model(inputs)
        loss = criterion(outputs, targets.view(-1, 1))
        loss.backward()
        optimizer.step()
        total_train_loss += loss.item()

    avg_train_loss = total_train_loss / len(train_loader)

    # Validation phase
    model.eval()
    total_val_loss = 0
    with torch.no_grad():
        for inputs, targets in val_loader:
            outputs = model(inputs)
            loss = criterion(outputs, targets.view(-1, 1))
            total_val_loss += loss.item()

    avg_val_loss = total_val_loss / len(val_loader)
    print(f'Epoch [{epoch+1}/{100}], Train Loss: {avg_train_loss:.4f}, Val Loss: {avg_val_loss:.4f}')

# Evaluate on test set
model.eval()
total_test_loss = 0
with torch.no_grad():
    for inputs, targets in DataLoader(dataset=test_dataset, batch_size=64, shuffle=False):
        outputs = model(inputs)
        loss = criterion(outputs, targets.view(-1, 1))
        total_test_loss += loss.item()

avg_test_loss = total_test_loss / len(DataLoader(dataset=test_dataset, batch_size=64))
test_rmse = torch.sqrt(torch.tensor(avg_test_loss))
print(f'Test RMSE: {test_rmse.item()}')


# In[23]:


# Hyperparameter tuning
best_rmse = float('inf')
best_lr = None
best_optimizer_type = None

for lr in [0.001, 0.01, 0.1]:
    for optimizer_type in [optim.Adam, optim.SGD]:
        model = MLP()
        optimizer = optimizer_type(model.parameters(), lr=lr)

        print(f"Training with LR: {lr} and Optimizer: {'Adam' if optimizer_type == optim.Adam else 'SGD'}")
        for epoch in range(100):  # 50 epochs for tuning
            model.train()
            for inputs, targets in train_loader:
                optimizer.zero_grad()
                outputs = model(inputs)
                loss = criterion(outputs, targets.view(-1, 1))
                loss.backward()
                optimizer.step()

        # Evaluate on validation set
        model.eval()
        with torch.no_grad():
            val_outputs = model(val_features_tensor)
            val_loss = torch.sqrt(criterion(val_outputs, val_targets_tensor.view(-1, 1)))
            val_rmse = val_loss.item()
        print(f"Validation RMSE: {val_rmse}")

        if val_rmse < best_rmse:
            best_rmse = val_rmse
            best_lr = lr
            best_optimizer_type = optimizer_type
            best_model_state = model.state_dict()

print(f'Best Learning Rate: {best_lr}, Best Optimizer: {best_optimizer_type.__name__}, Best RMSE: {best_rmse}')


# In[24]:


tuned_model = MLP()
tuned_model.load_state_dict(best_model_state)

# Evaluate the best model on test set
tuned_model.eval()
with torch.no_grad():
    test_outputs = tuned_model(test_features_tensor)
    test_loss = torch.sqrt(criterion(test_outputs, test_targets_tensor.view(-1, 1)))
    print(f'Test RMSE with Best Model: {test_loss.item()}')


# In[ ]:




