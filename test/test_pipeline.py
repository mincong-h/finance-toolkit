from finance_toolkit.account import (
    Account,
)
from finance_toolkit.bnp import (
    BnpAccount,
    BnpBalancePipeline,
    BnpTransactionPipeline,
)
from finance_toolkit.boursorama import (
    BoursoramaAccount,
    BoursoramaBalancePipeline,
    BoursoramaTransactionPipeline,
)
from finance_toolkit.fortuneo import FortuneoAccount, FortuneoTransactionPipeline
from finance_toolkit.pipeline import GeneralBalancePipeline, NoopTransactionPipeline
from finance_toolkit.revolut import (
    RevolutAccount,
    RevolutTransactionPipeline,
    RevolutBalancePipeline,
)
from finance_toolkit.pipeline_factory import PipelineFactory


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
        Account(
            account_type="unknown",
            account_id="unknown",
            account_num="unknown",
            currency="EUR",
            patterns=["unknown"],
        )
    )
    p_r1 = PipelineFactory(cfg).new_transaction_pipeline(
        RevolutAccount(
            account_type=RevolutAccount.TYPE_CASH,
            account_id="foo-REV-USD",
            account_num="unknown",
            currency="USD",
        )
    )
    p_r2 = PipelineFactory(cfg).new_transaction_pipeline(
        RevolutAccount(
            account_type=RevolutAccount.TYPE_CASH,
            account_id="foo-REV-EUR",
            account_num="unknown",
            currency="USD",
        )
    )
    p_r3 = PipelineFactory(cfg).new_transaction_pipeline(
        RevolutAccount(
            account_type=RevolutAccount.TYPE_COMMODITIES,
            account_id="foo-REV-GLD",
            account_num="unknown",
            currency="USD",
        )
    )

    assert isinstance(p1, BnpTransactionPipeline)
    assert isinstance(p2, BoursoramaTransactionPipeline)
    assert isinstance(p3, FortuneoTransactionPipeline)
    assert isinstance(p4, NoopTransactionPipeline)
    assert isinstance(p_r1, RevolutTransactionPipeline)
    assert isinstance(p_r2, RevolutTransactionPipeline)
    assert isinstance(p_r3, NoopTransactionPipeline)


def test_new_balance_pipeline(cfg):
    p1 = PipelineFactory(cfg).new_balance_pipeline(
        BnpAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p2 = PipelineFactory(cfg).new_balance_pipeline(
        BoursoramaAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p3 = PipelineFactory(cfg).new_balance_pipeline(
        Account(
            account_type="unknown",
            account_num="unknown",
            account_id="unknown",
            currency="EUR",
            patterns=["unknown"],
        )
    )
    p_r1 = PipelineFactory(cfg).new_balance_pipeline(
        RevolutAccount(
            account_type=RevolutAccount.TYPE_CASH,
            account_id="foo-REV-USD",
            account_num="unknown",
            currency="USD",
        )
    )
    p_r2 = PipelineFactory(cfg).new_balance_pipeline(
        RevolutAccount(
            account_type=RevolutAccount.TYPE_CASH,
            account_id="foo-REV-EUR",
            account_num="unknown",
            currency="USD",
        )
    )
    p_r3 = PipelineFactory(cfg).new_balance_pipeline(
        RevolutAccount(
            account_type=RevolutAccount.TYPE_COMMODITIES,
            account_id="foo-REV-GLD",
            account_num="unknown",
            currency="USD",
        )
    )

    assert isinstance(p1, BnpBalancePipeline)
    assert isinstance(p2, BoursoramaBalancePipeline)
    assert isinstance(p3, GeneralBalancePipeline)
    assert isinstance(p_r1, RevolutBalancePipeline)
    assert isinstance(p_r2, RevolutBalancePipeline)
    assert isinstance(p_r3, GeneralBalancePipeline)
