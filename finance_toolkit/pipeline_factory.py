from pathlib import Path

from .accounts import (
    Account,
    BnpAccount,
    BoursoramaAccount,
    FortuneoAccount,
)
from .models import Configuration
from .pipeline_bnp import BnpTransactionPipeline, BnpBalancePipeline
from .pipeline_boursorama import (
    BoursoramaTransactionPipeline,
    BoursoramaBalancePipeline,
)
from .pipelines import (
    TransactionPipeline,
    FortuneoTransactionPipeline,
    NoopTransactionPipeline,
    BalancePipeline,
    GeneralBalancePipeline,
    AccountParser,
)


class PipelineFactory:
    def __init__(self, cfg: Configuration):
        self.cfg = cfg

    def new_transaction_pipeline(self, account: Account) -> TransactionPipeline:
        if isinstance(account, BnpAccount):
            return BnpTransactionPipeline(account, self.cfg)
        if isinstance(account, BoursoramaAccount):
            return BoursoramaTransactionPipeline(account, self.cfg)
        if isinstance(account, FortuneoAccount):
            return FortuneoTransactionPipeline(account, self.cfg)
        return NoopTransactionPipeline(account, self.cfg)

    def new_balance_pipeline(self, account: Account) -> BalancePipeline:
        if isinstance(account, BnpAccount):
            return BnpBalancePipeline(account, self.cfg)
        if isinstance(account, BoursoramaAccount):
            return BoursoramaBalancePipeline(account, self.cfg)
        return GeneralBalancePipeline(account, self.cfg)

    def parse_balance_pipeline(self, path: Path) -> BalancePipeline:
        account = AccountParser(self.cfg).parse(path)
        return self.new_balance_pipeline(account)