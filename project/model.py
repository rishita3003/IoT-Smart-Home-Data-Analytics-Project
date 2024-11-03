from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import to_timestamp, year, month, dayofmonth, hour, minute
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class ModelTrainer:
    def __init__(self, data_path, model_save_path):
        self.spark = SparkSession.builder \
            .appName("Model Training and Selection for Regression") \
            .getOrCreate()
        self.data_path = data_path
        self.model_save_path = model_save_path
        self.target_col = "optimal_temperature"

    def load_and_preprocess_data(self):
        self.data = self.spark.read.csv(self.data_path, header=True, inferSchema=True)
        self.data = self.data.withColumn("time", to_timestamp("time", 'yyyy-MM-dd HH:mm:ss'))
        self.data = self.preprocess_data(self.data)

    def preprocess_data(self, df):
        df = df.withColumn("year", year("time")) \
               .withColumn("month", month("time")) \
               .withColumn("day", dayofmonth("time")) \
               .withColumn("hour", hour("time")) \
               .withColumn("minute", minute("time")) \
               .drop("time")
        # Drop any rows with null values in the columns that are input to the VectorAssembler
        df = df.na.drop(subset=[col for col in df.columns if col != self.target_col and col != "features"])

        feature_columns = [col for col in df.columns if col != self.target_col and col != "features"]
       
        # Check if the 'features' column already exists, if not create it
        if "features" not in df.columns:
            assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")
            
        return df

    def train_models(self):
        if "features" in self.data.columns:
            self.data = self.data.drop("features")
        train_data, test_data = self.data.randomSplit([0.8, 0.2], seed=42)
        gbt = GBTRegressor(featuresCol='features', labelCol=self.target_col)
        # Assemble transformations and the model into a Pipeline
        stages = [VectorAssembler(inputCols=[col for col in train_data.columns if col != self.target_col and col != "features"], outputCol="features"), gbt]
        pipeline = Pipeline(stages=stages)
        model = pipeline.fit(train_data)
        # Evaluate the model
        predictions = model.transform(test_data)
        evaluator = RegressionEvaluator(labelCol=self.target_col, predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        print(f"RMSE: {rmse}")
        # Save the entire pipeline
        model.write().overwrite().save(f"{self.model_save_path}/model_pipeline")
        print("Model saved")

    def load_model(self):
        self.model = PipelineModel.load(f"{self.model_save_path}/model_pipeline")

    def predict(self, data_pd):
        spark = SparkSession.builder.appName("Prediction").getOrCreate()
        # Convert Pandas DataFrame to Spark DataFrame
        data_spark = spark.createDataFrame(data_pd)
        # Now apply preprocessing and prediction
        data_preprocessed = self.preprocess_data(data_spark)
        predictions = self.model.transform(data_preprocessed)
        # Convert results back to Pandas for use in Streamlit
        result_pd = predictions.select("prediction").toPandas()
        return result_pd['prediction'][0]
    

    def run(self, load_existing_model=False, train_new_model=True):
        if load_existing_model:
            print("Loading existing model...")
            self.load_model()
        if train_new_model:
            print("Training new model...")
            self.load_and_preprocess_data()
            self.train_models()

if __name__ == "__main__":
    data_path = '/home/rishita/project/processed_data.csv'
    model_save_path = '/home/rishita/project'
    trainer = ModelTrainer(data_path, model_save_path)
    trainer.run(load_existing_model=False, train_new_model=True)
