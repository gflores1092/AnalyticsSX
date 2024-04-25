import pandas as pd
from email_utils import send_email
from whatsapp_utils import send_whatsapp_message
from css_styles import html_email_style

# Data Loading
def load_data(filepath):
    try:
        return pd.read_csv(filepath)
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

# Data Processing and Aggregation
def process_data(df):
    try:
        aggregated_data = df.groupby('sellerId').agg({'orderAmount': 'sum'}).reset_index()
        return aggregated_data
    except Exception as e:
        print(f"Error processing data: {e}")
        return None

# Check Conditions and Prepare Messages
def prepare_and_send_messages(data):
    for index, row in data.iterrows():
        if row['orderAmount'] > 10000:  # Example condition
            html_content = f"""
            <html>
            <head><style>{html_email_style}</style></head>
            <body>
            <h1>Order Summary</h1>
            <p>Seller ID: {row['sellerId']}</p>
            <p>Total Order Amount: {row['orderAmount']}</p>
            </body>
            </html>
            """
            try:
                send_email('receiver@example.com', 'Order Alert', html_content, 'your-email@example.com', 'your-password')
                send_whatsapp_message('+1234567890', f'Seller {row['sellerId']} has exceeded the order limit.')
            except Exception as e:
                print(f"Error sending messages: {e}")

# Main Execution
if __name__ == '__main__':
    df = load_data('path_to_your_data.csv')
    if df is not None:
        processed_data = process_data(df)
        if processed_data is not None:
            prepare_and_send_messages(processed_data)

