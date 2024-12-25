import time
from decimal import Decimal

import pandas as pd
import polars as pl

from api.binance import create_binance_market_api
from util import async_retry_getter, convert_interval_to_timedelta


def _get_from_filters(filters, filter_type, field_name):
    for f in filters:
        if f['filterType'] == filter_type:
            return f[field_name]


def _parse_usdt_futures_syminfo(info):
    filters = info['filters']
    return {
        'symbol': info['symbol'],
        'contract_type': info['contractType'],
        'status': info['status'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'margin_asset': info['marginAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(_get_from_filters(filters, 'LOT_SIZE', 'stepSize')),
        'min_notional_value': Decimal(_get_from_filters(filters, 'MIN_NOTIONAL', 'notional'))
    }


def _parse_coin_futures_syminfo(info):
    filters = info['filters']
    return {
        'symbol': info['symbol'],
        'contract_type': info['contractType'],
        'status': info['contractStatus'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'margin_asset': info['marginAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(info['contractSize'])
    }


def _parse_spot_syminfo(info):
    filters = info['filters']
    return {
        'symbol': info['symbol'],
        'status': info['status'],
        'base_asset': info['baseAsset'],
        'quote_asset': info['quoteAsset'],
        'price_tick': Decimal(_get_from_filters(filters, 'PRICE_FILTER', 'tickSize')),
        'lot_size': Decimal(_get_from_filters(filters, 'LOT_SIZE', 'stepSize')),
        'min_notional_value': Decimal(_get_from_filters(filters, 'NOTIONAL', 'minNotional'))
    }


class BinanceFetcher:

    TYPE_MAP = {
        'usdt_futures': _parse_usdt_futures_syminfo,
        'coin_futures': _parse_coin_futures_syminfo,
        'spot': _parse_spot_syminfo,
    }

    def __init__(self, type_, session):
        self.trade_type = type_
        self.market_api = create_binance_market_api(type_, session)

        if type_ in self.TYPE_MAP:
            self.syminfo_parse_func = self.TYPE_MAP[type_]
        else:
            raise ValueError(f'Type {type_} not supported')

    def get_api_limits(self) -> tuple[int, int]:
        return self.market_api.MAX_MINUTE_WEIGHT, self.market_api.WEIGHT_EFFICIENT_ONCE_CANDLES

    async def get_time_and_weight(self) -> tuple[pd.Timestamp, int]:
        server_timestamp, weight = await self.market_api.aioreq_time_and_weight()
        server_timestamp = pd.to_datetime(server_timestamp, unit='ms', utc=True)
        return server_timestamp, weight

    async def get_exchange_info(self) -> dict[str, dict]:
        """
        Parse trading rules from return values of /exchangeinfo API
        """
        exg_info = await async_retry_getter(self.market_api.aioreq_exchange_info)
        results = dict()
        for info in exg_info['symbols']:
            results[info['symbol']] = self.syminfo_parse_func(info)
        return results

    @staticmethod
    def get_candle_with_original_pandas(klines, interval) -> pd.DataFrame:
        columns = [
            'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ]
        df = pd.DataFrame(klines, columns=columns)
        df.drop(columns=['ignore', 'close_time'], inplace=True)
        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'].astype('int64'), unit='ms', utc=True)
        for col in [
            'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num', 'taker_buy_base_asset_volume',
            'taker_buy_quote_asset_volume'
        ]:
            df[col] = df[col].astype(float)

        df['candle_end_time'] = df['candle_begin_time'] + convert_interval_to_timedelta(interval)
        df.set_index('candle_end_time', inplace=True)
        return df

    @staticmethod
    def get_candle_with_pandas(klines, interval) -> pd.DataFrame:
        columns = [
            'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ]
        df = pd.DataFrame(klines, columns=columns)
        df.drop(columns=['ignore', 'close_time'], inplace=True)

        dtypes = {
            'candle_begin_time': int,
            'open': float,
            'high': float,
            'low': float,
            'close': float,
            'volume': float,
            'quote_volume': float,
            'trade_num': int,
            'taker_buy_base_asset_volume': float,
            'taker_buy_quote_asset_volume': float
        }

        df = df.astype(dtypes)

        df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms', utc=True)
        df.sort_values('candle_begin_time', ignore_index=True, inplace=True)
        df['candle_end_time'] = df['candle_begin_time'] + convert_interval_to_timedelta(interval)
        df.set_index('candle_end_time', inplace=True)
        return df

    @staticmethod
    def get_candle_with_polar(klines, interval) -> pd.DataFrame:
        schema = {
            'candle_begin_time': pl.Int64,
            'open': pl.Float64,
            'high': pl.Float64,
            'low': pl.Float64,
            'close': pl.Float64,
            'volume': pl.Float64,
            'close_time': None,
            'quote_volume': pl.Float64,
            'trade_num': pl.Int64,
            'taker_buy_base_asset_volume': pl.Float64,
            'taker_buy_quote_asset_volume': pl.Float64,
            'ignore': None
        }

        lf = pl.LazyFrame(klines, schema=schema, orient='row')
        lf = lf.drop('close_time', 'ignore')

        lf = lf.with_columns(pl.col('candle_begin_time').cast(pl.Datetime('ms')).dt.replace_time_zone('UTC'))
        lf = lf.sort('candle_begin_time')

        delta = convert_interval_to_timedelta(interval)
        lf = lf.with_columns((pl.col('candle_begin_time') + delta).alias('candle_end_time'))
        df = lf.collect().to_pandas()
        df['candle_end_time'] = df['candle_begin_time'] + convert_interval_to_timedelta(interval)
        df.set_index('candle_end_time', inplace=True)
        return df

    async def get_candle(self, symbol, interval, **kwargs) -> pd.DataFrame:
        '''
        Parse return values of /klines API and convert to pd.DataFrame
        '''
        data = await async_retry_getter(self.market_api.aioreq_klines, symbol=symbol, interval=interval, **kwargs)
        return self.get_candle_with_original_pandas(data, interval)

    async def get_funding_rate(self) -> pd.DataFrame:
        if self.trade_type == 'spot':
            raise RuntimeError('Cannot request funding rate for spot')
        data = await self.market_api.aioreq_premium_index()
        # 如果 lastFundingRate 不能转换为浮点数，则转换为 nan
        data = [{
            'symbol': d['symbol'],
            'fundingRate': pd.to_numeric(d['lastFundingRate'], errors='coerce')
        } for d in data]
        df = pd.DataFrame.from_records(data)
        return df
