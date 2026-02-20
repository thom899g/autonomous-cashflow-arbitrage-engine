import ccxt
from typing import Dict, List, Optional
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class ExchangeManager:
    """Manages multiple cryptocurrency exchanges and fetches OHLCV data."""
    
    def __init__(self, config: Dict):
        self.config = config
        self.exchanges = {}
        self._initialize_exchanges()
        
    def _initialize_exchanges(self) -> None:
        """Initializes exchange instances with API keys and secrets."""
        for exchange_name in self.config['exchanges']:
            try:
                if exchange_name == 'binance':
                    exchange = ccxt.binance({
                        'apiKey': self.config['api_keys']['binance_api_key'],
                        'secretKey': self.config['api_keys']['binance_secret']
                    })
                elif exchange_name == 'kucoin':
                    exchange = ccxt.kucoin({
                        'apiKey': self.config['api_keys']['kucoin_api_key'],
                        'secretKey': self.config['api_keys']['kucoin_secret'],
                        'password': self.config['api_keys']['kucoin_password']
                    })
                else:
                    logger.error(f"Exchange {exchange_name} not supported")
                    continue
                
                exchange.load_markets()
                self.exchanges[exchange_name] = exchange
            except Exception as e:
                logger.error(f"Failed to initialize {exchange_name}: {str(e)}")

    def get_price_data(self, symbol: str, timeframe: str = '1h') -> Dict[str, Dict]:
        """Fetches historical price data for a given symbol across all exchanges.
        
        Args:
            symbol: Cryptocurrency symbol (e.g., BTC/USDT)
            timeframe: Timeframe for OHLCV data
            
        Returns:
            Dictionary mapping exchange names to their respective price data
        """
        price_data = {}
        current_time = datetime.now()
        
        try:
            for exchange_name, exchange in self.exchanges.items():
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe)
                
                # Convert OHLCV to a dictionary with timestamps as keys
                data_dict = {
                    timestamp: {
                        'open': o[0],
                        'high': o[1],
                        'low': o[2],
                        'close': o[3],
                        'volume': o[4]
                    }
                    for timestamp, o in enumerate(ohlcv)
                }
                
                price_data[exchange_name] = data_dict
            return price_data
            
        except ccxt.BaseError as e:
            logger.error(f"Failed to fetch OHLCV data: {str(e)}")
            raise

    def get_current_price(self, symbol: str) -> Dict[str, float]:
        """Fetches the current price of a symbol across all exchanges.
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            Dictionary mapping exchange names to their current prices
        """
        try:
            prices = {}
            for exchange_name, exchange in self.exchanges.items():
                price = exchange.fetch_ticker(symbol)['bid']
                prices[exchange_name] = float(price)
            return prices
        except Exception as e:
            logger.error(f"Failed to fetch current price: {str(e)}")
            raise

    def get_volume_data(self, symbol: str) -> Dict[str, Dict]:
        """Fetches volume data for a given symbol across all exchanges.
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            Dictionary mapping exchange names to their volume data
        """
        try:
            volumes = {}
            for exchange_name, exchange in self.exchanges.items():
                ohlcv = exchange.fetch_ohlcv(symbol, '1h')
                # Convert OHLCV to a dictionary with timestamps as keys
                data_dict = {
                    timestamp: o[4]  # Volume is the 5th element (index 4)
                    for timestamp, o in enumerate(ohlcv)
                }
                volumes[exchange_name] = data_dict
            return volumes
        except Exception as e:
            logger.error(f"Failed to fetch volume data: {str(e)}")
            raise

    def get_market_depth(self, symbol: str) -> Dict[str, Dict]:
        """Fetches market depth for a given symbol across all exchanges.
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            Dictionary mapping exchange names to their order book
        """
        try:
            order_books = {}
            for exchange_name, exchange in self.exchanges.items():
                order_book = exchange.fetch_order_book(symbol)
                order_books[exchange_name] = {
                    'bids': order_book['bids'],
                    'asks': order_book['asks']
                }
            return order_books
        except Exception as e:
            logger.error(f"Failed to fetch market depth: {str(e)}")
            raise

    def get_exchange_info(self) -> Dict[str, Dict]:
        """Returns information about all supported exchanges.
        
        Returns:
            Dictionary mapping exchange names to their details
        """
        try:
            info = {}
            for exchange_name, exchange in self.exchanges.items():
                info[exchange_name] = {
                    'currencies': exchange.currencies,
                    'timeframes': exchange.timeframes,
                    'markets': len(exchange.markets)
                }
            return info
        except Exception as e:
            logger.error(f"Failed to fetch exchange info: {str(e)}")
            raise

    def get_historical_prices(self, symbol: str, timeframe: str = '1h', lookback: int = 24) -> Dict[str, Dict]:
        """Fetches historical prices for a given symbol across all exchanges.
        
        Args:
            symbol: Cryptocurrency symbol
            timeframe: Timeframe for OHLCV data
            lookback: Number of periods to look back
            
        Returns:
            Dictionary mapping exchange names to their historical price data
        """
        try:
            historical_data = {}
            end_time = datetime.now().timestamp()
            start_time = (end_time - (lookback * 60 * 60))  # Convert hours to seconds
            
            for exchange_name, exchange in self.exchanges.items():
                ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=start_time)
                
                data_dict = {
                    timestamp: {
                        'open': o[0],
                        'high': o[1],
                        'low': o[2],
                        'close': o[3],
                        'volume': o[4]
                    }
                    for timestamp, o in enumerate(ohlcv)
                }
                
                historical_data[exchange_name] = data_dict
            return historical_data
            
        except Exception as e:
            logger.error(f"Failed to fetch historical prices: {str(e)}")
            raise

    def get_last_traded_price(self, symbol: str) -> Dict[str, float]:
        """Fetches the last traded price for a given symbol across all exchanges.
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            Dictionary mapping exchange names to their last traded prices
        """
        try:
            prices = {}
            for exchange_name, exchange in self.exchanges.items():
                ticker = exchange.fetch_ticker(symbol)
                last_traded_price = ticker['last']
                prices[exchange_name] = float(last_traded_price)
            return prices
        except Exception as e:
            logger.error(f"Failed to fetch