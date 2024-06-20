from noahtrader.event import *
from noahtrader.data import *
from noahtrader.portfolio import *
from noahtrader.execution import *
from noahtrader.strategy import *

import time
import os
import random

if __name__ == '__main__':
    """
        data -> strategy -> portfolio -> execution
    """
    
    print("NoahTrader Is Running Successfully!")
    
    _events = Event()
    _bars = DataHandler()
    _strategy = Strategy()
    _portfolio = Portfolio()
    _broker = ExecutionHandler()
    
    
    while True:
        try:
            _event = _events.get()
            
        except:
            break
            
        if _event is not None:
            if _event.type == 'MARKET':
                pass
            elif _event.type == 'SIGNAL':
                pass
            elif _event.type == 'ORDER':
                pass
            elif _event.type == 'FILL':
                pass
            
            
        time.sleep(10)
    