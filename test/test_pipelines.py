from finance_toolkit.accounts import (
    Account,
    BnpAccount,
    BoursoramaAccount,
    FortuneoAccount,
)
from finance_toolkit.bnp import (
    BnpBalancePipeline,
    BnpTransactionPipeline,
)
from finance_toolkit.boursorama import (
    BoursoramaBalancePipeline,
    BoursoramaTransactionPipeline,
)
from finance_toolkit.pipeline_factory import PipelineFactory
from finance_toolkit.pipelines import (
    FortuneoTransactionPipeline,
    GeneralBalancePipeline,
    NoopTransactionPipeline,
)


# ---------- Class: AccountPipeline ----------


def test_new_transaction_pipeline(cfg):
    p1 = PipelineFactory(cfg).new_transaction_pipeline(
        BnpAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p2 = PipelineFactory(cfg).new_transaction_pipeline(
        BoursoramaAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p3 = PipelineFactory(cfg).new_transaction_pipeline(
        FortuneoAccount("CHQ", "foo-FTN-CHQ", "12345")
    )
    p4 = PipelineFactory(cfg).new_transaction_pipeline(
        Account("unknown", "unknown", "unknown", ["unknown"])
    )

    assert isinstance(p1, BnpTransactionPipeline)
    assert isinstance(p2, BoursoramaTransactionPipeline)
    assert isinstance(p3, FortuneoTransactionPipeline)
    assert isinstance(p4, NoopTransactionPipeline)


def test_new_balance_pipeline(cfg):
    p1 = PipelineFactory(cfg).new_balance_pipeline(
        BnpAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p2 = PipelineFactory(cfg).new_balance_pipeline(
        BoursoramaAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p3 = PipelineFactory(cfg).new_balance_pipeline(
        Account("unknown", "unknown", "unknown", ["unknown"])
    )

    assert isinstance(p1, BnpBalancePipeline)
    assert isinstance(p2, BoursoramaBalancePipeline)
    assert isinstance(p3, GeneralBalancePipeline)
