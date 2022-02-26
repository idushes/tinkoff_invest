import os
from datetime import datetime
from dotenv import load_dotenv
from tinkoff_helper import get_operations, get_total_portfolio_usd
import pandas as pd


load_dotenv()

from_ = datetime.fromisoformat(os.getenv('FROM'))
to_ = datetime.now()
account_id = "2000274502"
operations_filename = "operations.pkl"

if __name__ == '__main__':
    if os.path.isfile(operations_filename):
        df = pd.read_pickle(operations_filename)
    else:
        data = get_operations(account_id=account_id, from_=from_, to_=to_)
        df = pd.DataFrame(data)
        df.to_pickle(operations_filename)
    df = df.fillna(method='bfill')
    df = df.loc[df['operation_type'].isin([1, 9])]
    df_grouped = df.groupby(['currency']).sum()
    input_sum = df_grouped['usd'].sum()
    current_total_usd = get_total_portfolio_usd(account_id="2000274502")
    print(f"""
        START: {df['date'].min()}
        NET_INPUT: -${int(input_sum)}
        CURRENT: ${int(current_total_usd)}
        PROFFIT (USD): {int(current_total_usd - input_sum)}
    """)
