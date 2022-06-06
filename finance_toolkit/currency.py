class CurrencySymbol(str):
    def __init__(self, symbol: str):
        self.symbol: str = symbol


USD = CurrencySymbol("USD")

EUR = CurrencySymbol("EUR")
