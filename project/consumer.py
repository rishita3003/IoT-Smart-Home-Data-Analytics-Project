import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer
from model import ModelTrainer
import plotly.express as px

# Initialize Streamlit
st.title('Kafka Consumer Data Dashboard')
st.write('Streaming data from Kafka topic "raw-climate-data".')

# Global DataFrame to hold streaming data
global_df = pd.DataFrame()

# Setup Kafka Consumer
def consume_data_from_kafka(model_trainer):
    consumer = KafkaConsumer(
        'raw-climate-data',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None
    )

    for message in consumer:
        data = message.value
        if data:
            try:
                # Convert data to DataFrame, assume data is a dictionary
                df = pd.DataFrame([data])
                # Predict using the model trainer
                prediction = model_trainer.predict(df)
                df['prediction'] = prediction
                yield df
            except Exception as e:
                print(f"Error processing data: {e}")
                yield pd.DataFrame()
        else:
            yield pd.DataFrame()

def main():
    model_save_path = '/home/rishita/project'
    trainer = ModelTrainer(None, model_save_path)  # Assuming ModelTrainer can handle None for data_path
    trainer.load_model()  # Ensure the model is correctly loaded

    placeholder = st.empty()
    chart_placeholder = st.empty()
    
    # Streamlit component to display data
    global global_df
    for df in consume_data_from_kafka(trainer):
        if not df.empty:
            global_df = pd.concat([global_df, df], ignore_index=True)
            placeholder.dataframe(global_df)
            # Update the plot using Plotly
            fig = px.line(global_df, x=global_df.index, y=['temperature', 'prediction'],
                          labels={'value': 'Temperature Values', 'index': 'Data Point'},
                          title='Temperature and Predicted Optimal Temperature')
            fig.update_traces(mode='lines+markers')
            chart_placeholder.plotly_chart(fig, use_container_width=True)
        else:
            st.write("Waiting for data...")

if __name__ == "__main__":
    main()
