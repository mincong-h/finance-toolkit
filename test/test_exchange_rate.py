from pathlib import Path
from tempfile import TemporaryDirectory
from finance_toolkit.models import Summary
from finance_toolkit.pipeline_factory import PipelineFactory
from unittest.mock import patch
import datetime


def test_exchange_rate_pipeline_run(cfg):
    # Given
    with TemporaryDirectory() as root:
        csv = Path(root) / "Webstat_Export_20240107.csv"
        csv.write_text(
        """\
Titre :;Dollar australien (AUD);Lev bulgare (BGN);Real brésilien (BRL);Dollar canadien (CAD);Franc suisse (CHF);Yuan renminbi chinois (CNY);Livre chypriote (CYP);Couronne tchèque (CZK);Couronne danoise (DKK);Couronne estonienne (EEK);Livre sterling (GBP);Dollar de Hong Kong (HKD);Kuna croate (HRK);Forint hongrois (HUF);Roupie indonésienne (IDR);Sheqel israélien (ILS);Roupie Indienne (100 paise);Couronne islandaise (ISK);Yen japonais (JPY);Won coréen (KRW);Litas lituanien (LTL);Lats letton (LVL);Livre maltaise (MTL);Peso méxicain (MXN);Ringgit malaisien (MYR);Couronne norvégienne (NOK);Dollar neo-zélandais (NZD);Peso philippin (PHP);Zloty polonais (PLN);Leu roumain (RON);Rouble russe (RUB);Couronne suédoise (SEK);Dollar de Singapour (SGD);Tolar slovène (SIT);Couronne slovaque (SKK);Baht thaïlandais (THB);Livre turque (TRY);Dollar des Etats-Unis (USD);Rand sud-africain (ZAR)
Code série :;EXR.D.AUD.EUR.SP00.A;EXR.D.BGN.EUR.SP00.A;EXR.D.BRL.EUR.SP00.A;EXR.D.CAD.EUR.SP00.A;EXR.D.CHF.EUR.SP00.A;EXR.D.CNY.EUR.SP00.A;EXR.D.CYP.EUR.SP00.A;EXR.D.CZK.EUR.SP00.A;EXR.D.DKK.EUR.SP00.A;EXR.D.EEK.EUR.SP00.A;EXR.D.GBP.EUR.SP00.A;EXR.D.HKD.EUR.SP00.A;EXR.D.HRK.EUR.SP00.A;EXR.D.HUF.EUR.SP00.A;EXR.D.IDR.EUR.SP00.A;EXR.D.ILS.EUR.SP00.A;EXR.D.INR.EUR.SP00.A;EXR.D.ISK.EUR.SP00.A;EXR.D.JPY.EUR.SP00.A;EXR.D.KRW.EUR.SP00.A;EXR.D.LTL.EUR.SP00.A;EXR.D.LVL.EUR.SP00.A;EXR.D.MTL.EUR.SP00.A;EXR.D.MXN.EUR.SP00.A;EXR.D.MYR.EUR.SP00.A;EXR.D.NOK.EUR.SP00.A;EXR.D.NZD.EUR.SP00.A;EXR.D.PHP.EUR.SP00.A;EXR.D.PLN.EUR.SP00.A;EXR.D.RON.EUR.SP00.A;EXR.D.RUB.EUR.SP00.A;EXR.D.SEK.EUR.SP00.A;EXR.D.SGD.EUR.SP00.A;EXR.D.SIT.EUR.SP00.A;EXR.D.SKK.EUR.SP00.A;EXR.D.THB.EUR.SP00.A;EXR.D.TRY.EUR.SP00.A;EXR.D.USD.EUR.SP00.A;EXR.D.ZAR.EUR.SP00.A
Unité :;Dollar Australien (AUD);Lev Nouveau (BGN);Real Bresilien (BRL);Dollar Canadien (CAD);Franc Suisse (CHF);Yuan Ren Min Bi (CNY);Livre Cypriote (CYP);Couronne Tcheque (CZK);Couronne Danoise (DKK);Couronne d`Estonie (EEK);Livre Sterling (GBP);Dollar de Hong Kong (HKD);Kuna Croate (HRK);Forint (HUF);Rupiah (IDR);Nouveau Israeli Shekel (ILS);Roupie Indienne (INR);Couronne Islandaise (ISK);Yen (JPY);Won (KRW);Litas Lituanien (LTL);Lats Letton (LVL);Livre Maltaise (MTL);Nouveau Peso Mexicain (MXN);Ringgit de Malaisie (MYR);Couronne Norvegienne (NOK);Dollar Neo-Zelandais (NZD);Peso Philippin (PHP);Zloty (PLN);Nouveau Ron (RON);Rouble Russe (RUB) (RUB);Couronne Suedoise (SEK);Dollar de Singapour (SGD);Tolar (SIT);Couronne Slovaque (SKK);Baht (THB);Nouvelle Livre Turque (TRY);Dollar des Etats-Unis (USD);Rand (ZAR)
Magnitude :;Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0);Unités (0)
Méthode d'observation :;Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Moyenne de la période (A);Fin de période (E);Fin de période (E);Moyenne de la période (A);Fin de période (E);Fin de période (E);Moyenne de la période (A);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Moyenne de la période (A);Moyenne de la période (A);Moyenne de la période (A);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E);Moyenne de la période (A);Moyenne de la période (A);Fin de période (E);Fin de période (E);Fin de période (E);Fin de période (E)
Source :;BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0)
05/01/2024;1,6337;1,9558;5,3724;1,46;0,932;7,813;;24,616;7,4584;;0,8621;8,5297;;378,23;16967,41;4,0194;90,81;150,5;158,57;1439,64;;;;18,6066;5,0826;11,309;1,7564;60,704;4,3568;4,9737;;11,235;1,4537;;;37,994;32,5888;1,0921;20,5511
04/01/2024;1,628;1,9558;5,3761;1,4603;0,9313;7,833;;24,652;7,459;;0,86278;8,5523;;378,85;16994,46;3,9973;91,1745;150,5;157,91;1434,25;;;;18,6124;5,0762;11,2845;1,7528;60,833;4,346;4,9733;;11,1905;1,4546;;;37,75;32,6087;1,0953;20,4271
03/01/2024;1,6236;1,9558;5,3859;1,4574;0,9322;7,8057;;24,675;7,4581;;0,8647;8,5257;;380,75;16994,33;3,9867;90,965;150,7;156,16;1432,28;;;;18,6682;5,0566;11,32;1,7515;60,699;4,3638;4,9725;;11,1915;1,4503;;;37,616;32,5178;1,0919;20,5326
02/01/2024;1,6147;1,9558;5,3562;1,4565;0,9305;7,8264;;24,687;7,4551;;0,86645;8,5609;;382,1;17007,66;3,9705;91,285;150,7;155,68;1438,78;;;;18,6887;5,0425;11,2815;1,7471;60,981;4,3708;4,9705;;11,1545;1,4533;;;37,563;32,5684;1,0956;20,3656
"""  # noqa
    )
        with patch("finance_toolkit.exchange_rate.get_today", return_value=datetime.datetime(2024, 1, 6)):
            pipeline = PipelineFactory(cfg).new_exchange_rate_pipeline()
            summary = Summary(cfg)

            # When
            pipeline.run(csv, summary)

            # Then
            assert (cfg.root_dir / "exchange-rate.csv").exists()
            assert (cfg.root_dir / "exchange-rate.csv").read_text() == """\
Date,USD,CNY
2024-01-02,1.0956,7.8264
2024-01-03,1.0919,7.8057
2024-01-04,1.0953,7.833
2024-01-05,1.0921,7.813
2024-01-06,,
"""
