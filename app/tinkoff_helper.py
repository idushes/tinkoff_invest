import os
from datetime import datetime
from tinkoff.invest import Client, OperationsResponse, PortfolioResponse, OperationState
from currency_converter import CurrencyConverter, RateNotFoundError
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.environ["INVEST_TOKEN"]


# print(client.users.get_accounts())
# print(client.users.get_info())
# print(client.users.get_user_tariff())
# print(client.market_data.get_trading_status(figi='BBG000C496P7'))
# print(client.instruments.currencies())
# portfolio: PortfolioResponse = client.operations.get_portfolio(account_id="2000274502")


def get_operations(account_id: str, from_: datetime, to_: datetime) -> list[dict]:
    data: list[dict] = []
    with Client(TOKEN) as client:
        operations: OperationsResponse = client.operations.get_operations(account_id=account_id, from_=from_, to=to_,
                                                                          state=OperationState.OPERATION_STATE_EXECUTED)
        operations.operations.sort(key=lambda v: v.date, reverse=True)
        for operation in tqdm(operations.operations):
            payment = float(f"{operation.payment.units}.{abs(operation.payment.nano)}")
            currency = operation.payment.currency
            time_delta = datetime.now() - operation.date.replace(tzinfo=None)
            c = CurrencyConverter()
            try:
                usd = payment if currency == "usd" else c.convert(payment, currency.upper(), 'USD', date=operation.date)
            except RateNotFoundError:
                usd = c.convert(payment, currency.upper(), 'USD') if time_delta.days < 30 else None
            data.append({
                "id": operation.id,
                "figi": operation.figi,
                "date": operation.date,
                "currency": currency,
                "payment": payment,
                "type": operation.type,
                "operation_type": operation.operation_type.value,
                "instrument_type": operation.instrument_type,
                "usd": usd
            })
    return data


def get_total_portfolio_usd(account_id: str):
    # portfolio: PortfolioResponse = client.operations.get_portfolio(account_id="2000274502")
    return 93_986.11
