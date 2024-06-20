import datetime
import os
import os.path
import pandas as pd
from abc import ABCMeta, abstractmethod
from event import MarketEvent

class DataHandler(metaclass = ABCMeta):
    """
    作为一个抽象基类，提供处理数据的接口

    Args:
        metaclass (_type_, optional): _description_. Defaults to ABCMeta.

    Raises:
        NotImplementedError: _description_
        NotImplementedError: _description_
    """
    @abstractmethod
    def get_lastest_bars(self, symbol, N=1):
        
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):

        raise NotImplementedError("Should implement update_bars()")
    
class HistoricCSVDataHandler(DataHandler):
    
    def __init__(self, events, csv_dir, symbol_list) -> None:
        
        super().__init__()
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        
        self._open_convert_csv_files()
    
    def _open_convert_csv_files(self):
        
        comb_index = None
        
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.read_csv(
                os.path.join(self.csv_dir, "%s.csv".format(s)),
                header=0, index_col=0, parse_dates=True,
                names=['datetime', 'open', 'high', 
                       'low', 'close', 'adj_close', 'volume']
            )
            self.symbol_data[s].sort_index(inplace=True)
            
            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)
            
            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []
            
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(
                index=comb_index, method='pad'
            )
            self.symbol_data[s]["returns"] = self.symbol_data[s]["adj_close"].pct_change().dropna()
            self.symbol_data[s] = self.symbol_data[s].iterrows()
        
        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad').iterrows()
            
    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed as a tuple of 
        (sybmbol, datetime, open, low, high, close, volume).
        Args:
            symbol (_type_):    
        """
        
        for b in self.symbol_data[symbol]:
            yield tuple([symbol, datetime.datetime.strptime(b[0], '%Y-%m-%d %H:%M:%S'),
                          b[1][0], b[1][1], b[1][2], b[1][3], b[1][4]])
            
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        Args:
            symbol (_type_): _description_
            N (int, optional): _description_. Defaults to 1.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available")
        else:
            return bars_list[-N:]
        
    
    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        
        for s in self.symbol_list:
            try:
                bar = self._get_new_bar(s).next()
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
                    
            self.events.put(MarketEvent())