import streamlit as st
import pandas as pd
import json
from kafka import KafkaConsumer
from model import ModelTrainer
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime

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
                data['timestamp'] = datetime.datetime.now()
                df = pd.DataFrame([data])
                prediction = model_trainer.predict(df)
                df['prediction'] = prediction
                return df
            except Exception as e:
                print(f"Error processing data: {e}")
                return pd.DataFrame()
        else:
            return pd.DataFrame()

def main():
    model_save_path = '/home/rishita/project'
    trainer = ModelTrainer(None, model_save_path)
    trainer.load_model()

    table_placeholder = st.empty()
    chart_placeholder = st.empty()
    forecast_placeholder = st.empty()
    latest_placeholder = st.empty()
    alert_placeholder = st.empty()

    global global_df
    while True:
        df = consume_data_from_kafka(trainer)
        if not df.empty:
            global_df = pd.concat([global_df, df], ignore_index=True)
            global_df['timestamp'] = pd.to_datetime(global_df['timestamp'])  # Ensure timestamp conversion

            # Display dataframe
            table_placeholder.dataframe(global_df[['time', 'temperature', 'prediction']])

            # Create main subplot
            fig = make_subplots(rows=2, cols=2,
                                subplot_titles=("Real-time Temperature and Prediction",
                                                "Histogram of Temperature",
                                                "Scatter of Temperature vs. Prediction",
                                                "Historical Data"))

            # Plot 1: Real-time temperature and predictions
            fig.add_trace(go.Scatter(x=global_df['timestamp'], y=global_df['temperature'], mode='lines+markers', name='Temperature'), row=1, col=1)
            fig.add_trace(go.Scatter(x=global_df['timestamp'], y=global_df['prediction'], mode='lines+markers', name='Predicted Temperature'), row=1, col=1)

            # Plot 2: Histogram of temperatures
            fig.add_trace(go.Histogram(x=global_df['temperature'], name='Temperature Distribution'), row=1, col=2)

            # Plot 3: Scatter of temperature vs. prediction
            fig.add_trace(go.Scatter(x=global_df['temperature'], y=global_df['prediction'], mode='markers', name='Temp vs. Pred'), row=2, col=1)

            # Plot 4: Historical plot of temperature
            fig.add_trace(go.Scatter(x=global_df['timestamp'], y=global_df['temperature'], mode='lines', name='Historical Temperature'), row=2, col=2)

            fig.update_layout(height=800, showlegend=True)
            chart_placeholder.plotly_chart(fig, use_container_width=True)

            # Separate forecast plot
            forecast_fig = go.Figure()
            global_df['MA Prediction'] = global_df['temperature'].rolling(window=10).mean()
            forecast_fig.add_trace(go.Scatter(x=global_df['timestamp'], y=global_df['MA Prediction'], mode='lines', name='Moving Avg Temp'))
            forecast_fig.update_layout(title="Forecasting Future Temperatures", xaxis_title='Timestamp', yaxis_title='Value', height=400)
            forecast_placeholder.plotly_chart(forecast_fig, use_container_width=True)

            # Latest data points
            latest_fig = go.Figure()
            latest_fig.add_trace(go.Scatter(x=[global_df['timestamp'].iloc[-1]], y=[global_df['temperature'].iloc[-1]], mode='markers', name='Latest Temperature'))
            latest_fig.add_trace(go.Scatter(x=[global_df['timestamp'].iloc[-1]], y=[global_df['prediction'].iloc[-1]], mode='markers', name='Latest Prediction'))
            latest_fig.update_layout(title="Latest Temperature and Prediction", xaxis_title='Timestamp', yaxis_title='Value', height=400)
            latest_placeholder.plotly_chart(latest_fig, use_container_width=True)

            # Anomaly Detection (simple thresholding for illustration)
            anomalies = global_df[global_df['temperature'] > global_df['temperature'].quantile(0.99)]
            if not anomalies.empty:
                alert_placeholder.write(f"Anomaly Detected at {anomalies.iloc[-1]['timestamp']}: Temperature = {anomalies.iloc[-1]['temperature']}")

        else:
            st.write("Waiting for data...")

if __name__ == "__main__":
    main()
