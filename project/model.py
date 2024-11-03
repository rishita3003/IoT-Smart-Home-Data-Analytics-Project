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
        feature_columns = [col for col in df.columns if col != self.target_col]
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        return assembler.transform(df)

    def train_models(self):
        if "features" in self.columns:
            self.data = self.data.drop("features")
        train_data, test_data = self.data.randomSplit([0.8, 0.2], seed=42)
        gbt = GBTRegressor(featuresCol='features', labelCol=self.target_col)
        

        # Assemble transformations and the model into a Pipeline
        stages = [VectorAssembler(inputCols=[col for col in train_data.columns if col != self.target_col], outputCol="features"), gbt]
        pipeline = Pipeline(stages=stages)
        model = pipeline.fit(train_data)

        # Evaluate the model
        predictions = model.transform(test_data)
        evaluator = RegressionEvaluator(labelCol=self.target_col, predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)
        print(f"RMSE: {rmse}")

        # Save the entire pipeline
        model.save(f"{self.model_save_path}/model_pipeline")
        print(f"model saved")

    def load_model(self):
        self.model = PipelineModel.load(f"{self.model_save_path}/model_pipeline")

    def predict(self, data_json):
        schema = StructType([
            StructField("time", StringType(), True),
            StructField("temperature", DoubleType(), True)
        ])
        row_df = self.spark.createDataFrame([data_json], schema)
        row_df = self.preprocess_data(row_df)
        predictions = self.model.transform(row_df)
        return predictions.select("prediction").collect()[0]["prediction"]

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
