import pandas as pd
import logging

logger = logging.getLogger("DataIngestion")

def format_alice_blue_historical(raw_data):
    """
    Takes raw historical data from Alice Blue and converts it to a standard pandas DataFrame.
    """
    if not raw_data:
        logger.warning("No raw data received to format.")
        return pd.DataFrame()
        
    try:
        # Alice Blue returns a dictionary with 'result' containing a list of candles
        # Typically formatted as [datetime, open, high, low, close, volume]
        if isinstance(raw_data, dict) and 'result' in raw_data:
            df = pd.DataFrame(raw_data['result'])
            
            # Formatting timestamp 
            # AliceBlue SDK might return standard datetime strings or objects
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
            elif 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                df.set_index('time', inplace=True)

            # Ensure numeric types
            cols_to_convert = ['open', 'high', 'low', 'close', 'volume']
            for col in cols_to_convert:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
            return df
        else:
             logger.warning("Unrecognized data format from broker")
             return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error formatting raw data: {e}")
        return pd.DataFrame()
