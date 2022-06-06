class CurrencySymbol(str):
    def __init__(self, symbol: str):
        # TODO add warning for untested symbol
        self.symbol: str = symbol


USD = CurrencySymbol("USD")

EUR = CurrencySymbol("EUR")
