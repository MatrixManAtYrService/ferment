from ferment.binlog import span, summarize

BINLOG = """\
# at 4
#261025  1:05:02 server id 1  end_log_pos 300 CRC32 0x0 	Update_rows: table id 91 flags: STMT_END_F
### UPDATE `billing_bookkeeper`.`processing_group_dates`
### WHERE
###   @1=1
###   @2='2026-10-24'
###   @3='2026-10-23'
### SET
###   @1=1
###   @2='2026-10-25'
###   @3='2026-10-24'
### INSERT INTO `billing_bookkeeper`.`fee_ctd`
### SET
###   @1=10
###   @2=5.00
### INSERT INTO `billing_bookkeeper`.`fee_ctd`
### SET
###   @1=11
###   @2=7.00
# at 300
### DELETE FROM `billing_event`.`cmm_cache`
### WHERE
###   @1=3
COMMIT/*!*/;
"""


def test_a_runs_writes_are_read_back_per_table_with_changed_columns_named():
    cols = {("billing_bookkeeper", "processing_group_dates"): ["id", "cycle_date", "last_cycle_date"]}
    out = {
        f"{t['database']}.{t['table']}": t
        for t in summarize(BINLOG.splitlines(True), lambda d, t: cols.get((d, t), []))
    }
    pgd = out["billing_bookkeeper.processing_group_dates"]
    assert (pgd["updates"], pgd["changed_columns"]) == (1, {"cycle_date": 1, "last_cycle_date": 1})
    assert pgd["samples"] == [
        {"cycle_date": ["'2026-10-24'", "'2026-10-25'"], "last_cycle_date": ["'2026-10-23'", "'2026-10-24'"]}
    ]
    assert out["billing_bookkeeper.fee_ctd"]["inserts"] == 2
    assert out["billing_event.cmm_cache"]["deletes"] == 1
    assert list(out)[0] == "billing_bookkeeper.fee_ctd"  # most rows first


def test_a_span_crosses_binlog_files():
    names = ["binlog.000014", "binlog.000015", "binlog.000016"]
    assert span(names, "binlog.000015:12308", None) == [("binlog.000015", 12308, None), ("binlog.000016", None, None)]
    assert span(names, "binlog.000014:4", "binlog.000015:900") == [
        ("binlog.000014", 4, None),
        ("binlog.000015", None, 900),
    ]
