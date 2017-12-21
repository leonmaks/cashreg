""" Cashreg Repl 171215_1150 """
import logging.config
import re, copy
import psycopg2.errorcodes

import mod
_t = mod.Mod("tittles")          # pylint: disable-msg=C0103
_dbc = mod.Mod("db_core")        # pylint: disable-msg=C0103
_dbu = mod.Mod("db_util")        # pylint: disable-msg=C0103

def mods_rld(recursive=False):
    """Reload modules"""
    _t.reload()
    _dbc.reload()
    _dbu.reload()
    if recursive:
        _dbc.m.mods_rld(recursive)
        _dbu.m.mods_rld(recursive)


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s: %(asctime)s %(module)s:%(lineno)d(%(funcName)s) %(message)s", level=logging.DEBUG)
#     import cashreg_conf
#     logging.config.dictConfig(cashreg_conf.LOG_CONFIG)

_log = logging.getLogger(__name__)


# DEFS

# Replication administrative table
# REPL_ADM_CLIENT_TABLE = "repl$adm_client"

__CLI_REPL_TABLE_PFX = "r$t__"
__SRV_REPL_UP_TABLE_PFX = "r$u%03d_"
__SRV_REPL_DOWN_TABLE_PFX = "r$d%03d_"
__SRV_UPLD_TABLE_PFX = "r$_"

__REPL_FUNC_I_PFX = "r$fi_"
__REPL_FUNC_U_PFX = "r$fu_"
__REPL_FUNC_D_PFX = "r$fd_"

__REPL_TRIG_I_PFX = "r$ti_"
__REPL_TRIG_U_PFX = "r$tu_"
__REPL_TRIG_D_PFX = "r$td_"

__REPL_COL_PFX = "r$_"

__REPL_COL_ID = {"col_name": "%sid" % __REPL_COL_PFX, "col_type": "serial"}
__REPL_COL_ID_NO_SEQ = {"col_name": "%sid" % __REPL_COL_PFX, "col_type": "integer"}
__REPL_COL_OP = {"col_name": "%sop" % __REPL_COL_PFX, "col_type": "character varying(1)"}
__REPL_COL_CREATE_DT = {"col_name": "%screate_dt" % __REPL_COL_PFX, "col_type": "timestamp"}

__REPL_OP_NEW = "N"
__REPL_OP_UPDATE = "U"
__REPL_OP_DELETE = "D"
__REPL_OP_INIT = "I"


def __cashreg_list(conn, **kwargs):
    cashregs_ = []
    for c_ in conn.select_all("SELECT c.id, c.identity, c.siteguid FROM ffba_cashreg AS c", **kwargs):
        cashregs_.append({"id": c_[0], "identity": c_[1], "siteguid": c_[2]})
    return cashregs_


def filter_repl_cols(columns):
    return _t.m.list_filter_regex(columns, "^"+ re.escape(__REPL_COL_PFX))


def __cli_repl_table_name(table_name):
    return __CLI_REPL_TABLE_PFX + table_name


def __srv_repl_up_tname(table_name, cashreg_id):
    return __SRV_REPL_UP_TABLE_PFX % cashreg_id + table_name


def __srv_repl_down_table_name(table_name, cashreg_id):
    return __SRV_REPL_DOWN_TABLE_PFX % cashreg_id + table_name


def __srv_upld_tname(table_name):
    return __SRV_UPLD_TABLE_PFX + table_name


def __apply_commit(ctx, **kwargs):
    if ctx["srv"]["stmt"]: ctx["srv"]["conn"].execute_batch(ctx["srv"]["stmt"], **kwargs)
    if ctx["cli"]["stmt"]: ctx["cli"]["conn"].execute_batch(ctx["cli"]["stmt"], **kwargs)
    ctx["srv"]["conn"].commit()
    ctx["cli"]["conn"].commit()
    if ctx["srv"]["stmt"] or ctx["cli"]["stmt"]:
        _log.info((
            "Server CONN:'%s', STMT:%sClient CONN:'%s', STMT:%s"
        ) % (
            ctx["srv"]["conn"],
            ctx["srv"]["stmt"] and ("\n%s\n" % "\n".join(ctx["srv"]["stmt"])) or " [] ",
            ctx["cli"]["conn"],
            ctx["cli"]["stmt"] and ("\n%s" % "\n".join(ctx["cli"]["stmt"])) or " []",
        ))
    ctx["srv"]["stmt"] = []
    ctx["cli"]["stmt"] = []


def __cli_init_repl_table(conn, table_name, columns, **kwargs):
    stmt_ = (
        "INSERT INTO {repl_table_pfx}{table_name} ({columns}, {repl_col_op}, {repl_col_create_dt})"
        " SELECT {columns}, '{repl_op_init}', current_timestamp FROM {table_name}"
    ).format(
        repl_table_pfx=__CLI_REPL_TABLE_PFX,
        table_name=table_name,
        columns=", ".join(columns),
        repl_col_op=__REPL_COL_OP["col_name"],
        repl_col_create_dt=__REPL_COL_CREATE_DT["col_name"],
        repl_op_init=__REPL_OP_INIT,
    )
    if kwargs.get("apply"): conn.execute(stmt_, **kwargs)
    return [stmt_, ]


def __cli_create_func_after_insert(conn, table_name, columns, **kwargs):
    ddl_ = (
        "CREATE{replace} FUNCTION {repl_func_i_pfx}{table_name}()"
        " RETURNS TRIGGER AS $$ BEGIN"
        " INSERT INTO {repl_table_pfx}{table_name}"
        " ({columns}, {repl_col_op}, {repl_col_create_dt})"
        " VALUES ({pfx_cols}, '{repl_op_insert}', current_timestamp);"
        " RETURN NEW; END; $$ LANGUAGE plpgsql"
    ).format(
        replace=(kwargs.get("replace") and " OR REPLACE" or ""),
        repl_func_i_pfx=__REPL_FUNC_I_PFX,
        table_name=table_name,
        repl_table_pfx=__CLI_REPL_TABLE_PFX,
        columns=", ".join(columns),
        repl_col_op=__REPL_COL_OP["col_name"],
        repl_col_create_dt=__REPL_COL_CREATE_DT["col_name"],
        pfx_cols=", ".join(["NEW." + c_ for c_ in columns]), #
        repl_op_insert=__REPL_OP_NEW,
    )
    if kwargs.get("apply"): conn.execute(ddl_, **kwargs)
    return [ddl_, ]


def __cli_create_func_after_update(conn, table_name, columns, **kwargs):
    ddl_ = (
        "CREATE{replace} FUNCTION {repl_func_u_pfx}{table_name}()"
        " RETURNS TRIGGER AS $$ BEGIN"
        " INSERT INTO {repl_table_pfx}{table_name}"
        " ({columns}, {repl_col_op}, {repl_col_create_dt})"
        " VALUES ({pfx_cols}, '{repl_op_update}', current_timestamp);"
        " RETURN NEW; END; $$ LANGUAGE plpgsql"
    ).format(
        replace=(kwargs.get("replace") and " OR REPLACE" or ""),
        repl_func_u_pfx=__REPL_FUNC_U_PFX,
        table_name=table_name,
        repl_table_pfx=__CLI_REPL_TABLE_PFX,
        columns=", ".join(columns),
        repl_col_op=__REPL_COL_OP["col_name"],
        repl_col_create_dt=__REPL_COL_CREATE_DT["col_name"],
        pfx_cols=", ".join(["NEW." + c_ for c_ in columns]),
        repl_op_update=__REPL_OP_UPDATE,
    )
    if kwargs.get("apply"): conn.execute(ddl_, **kwargs)
    return [ddl_, ]


def __cli_create_func_before_delete(conn, table_name, columns, **kwargs):
    ddl_ = (
        "CREATE{replace} FUNCTION {repl_func_d_pfx}{table_name}()"
        " RETURNS TRIGGER AS $$ BEGIN"
        " INSERT INTO {repl_table_pfx}{table_name}"
        " ({columns}, {repl_col_op}, {repl_col_create_dt})"
        " VALUES ({pfx_cols}, '{repl_op_delete}', current_timestamp);"
        " RETURN OLD; END; $$ LANGUAGE plpgsql;"
    ).format(
        replace=(kwargs.get("replace") and " OR REPLACE" or ""),
        repl_func_d_pfx=__REPL_FUNC_D_PFX,
        table_name=table_name,
        repl_table_pfx=__CLI_REPL_TABLE_PFX,
        columns=", ".join(columns),
        repl_col_op=__REPL_COL_OP["col_name"],
        repl_col_create_dt=__REPL_COL_CREATE_DT["col_name"],
        pfx_cols=", ".join(["OLD." + c_ for c_ in columns]),
        repl_op_delete=__REPL_OP_DELETE,
    )
    if kwargs.get("apply"): conn.execute(ddl_, **kwargs)
    return [ddl_, ]


def __cli_create_trig_after_insert(conn, table_name, **kwargs):
    ddls_ = []
    if kwargs.get("replace"):
        ddls_ += __cli_drop_trig_after_insert(conn, table_name, **kwargs)
    ddl_ = (
        "CREATE TRIGGER {repl_trig_i_pfx}{table_name}"
        " AFTER INSERT ON {table_name}"
        " FOR EACH ROW EXECUTE PROCEDURE {repl_func_i_pfx}{table_name}()"
    ).format(
        repl_trig_i_pfx=__REPL_TRIG_I_PFX,
        table_name=table_name,
        repl_func_i_pfx=__REPL_FUNC_I_PFX
    )
    if kwargs.get("apply"): conn.execute(ddl_, **kwargs)
    ddls_.append(ddl_)
    return ddls_


def __cli_create_trig_after_update(conn, table_name, **kwargs):
    ddls_ = []
    if kwargs.get("replace"):
        ddls_ += __cli_drop_trig_after_update(conn, table_name, **kwargs)
    ddl_ = (
        "CREATE TRIGGER {repl_trig_u_pfx}{table_name}"
        " AFTER UPDATE ON {table_name}"
        " FOR EACH ROW EXECUTE PROCEDURE {repl_func_u_pfx}{table_name}()"
    ).format(
        repl_trig_u_pfx=__REPL_TRIG_U_PFX,
        table_name=table_name,
        repl_func_u_pfx=__REPL_FUNC_U_PFX
    )
    if kwargs.get("apply"): conn.execute(ddl_, **kwargs)
    ddls_.append(ddl_)
    return ddls_


def __cli_create_trig_before_delete(conn, table_name, **kwargs):
    ddls_ = []
    if kwargs.get("replace"):
        ddls_ += __cli_drop_trig_before_delete(conn, table_name, **kwargs)
    ddl_ = (
        "CREATE TRIGGER {repl_trig_d_pfx}{table_name}"
        " BEFORE DELETE ON {table_name}"
        " FOR EACH ROW EXECUTE PROCEDURE {repl_func_d_pfx}{table_name}()"
    ).format(
        repl_trig_d_pfx=__REPL_TRIG_D_PFX,
        table_name=table_name,
        repl_func_d_pfx=__REPL_FUNC_D_PFX
    )
    if kwargs.get("apply"): conn.execute(ddl_, **kwargs)
    ddls_.append(ddl_)
    return ddls_


def __srv_insert_repl_table_record(conn, cashreg_id, table_name, **kwargs):
    stmt_ = "INSERT INTO ffba_cashreg_repl_table (cashreg_id, table_name) VALUES (%s, '%s')" % (cashreg_id, table_name)
    if kwargs.get("apply"): conn.execute(stmt_, **kwargs)
    return [stmt_, ]


# TODO: do reload modules
def create_repl_objects(ctx, **kwargs):
    """Create replication objects for all user tables"""

    # List owned tables that are not replicating yet
    def __client_nonrepl_tables(conn, **kwargs):
        # Retrieve all tables owned by current user
        all_tables_ = _t.m.dalv(_dbu.m.db_owned_tables(conn, **kwargs), "table_name")
        # Divide tables across 2 lists: Normal Tables (nt_) & Replication Tables (rt_)
        re_ = "^%s" % re.escape(__CLI_REPL_TABLE_PFX) # Replication table prefix pattern
        nt_ = []
        rt_ = []
        for a_ in all_tables_:
            if re.search(re_, a_): rt_.append(a_)
            else: nt_.append(a_)
        # Return Normal Tables (nt_) not having Replication Tables (rt_)
        return [t_ for t_ in nt_ if __CLI_REPL_TABLE_PFX + t_ not in rt_]

    # Create for the table both client-side and server-side replication objects
    def __create_table_repl_objects(ctx, table_name, **kwargs):
        col_defs_ = _dbu.m.db_table_columns(ctx["cli"]["conn"], table_name, **kwargs)
        cols_ = _t.m.dalv(col_defs_, "col_name")

        # Create client repl table
        ctx["cli"]["stmt"] += _dbu.m.db_table_ddl(
            ctx["cli"]["conn"], table_name, col_defs_ + [__REPL_COL_ID, __REPL_COL_OP, __REPL_COL_CREATE_DT],
            None, None, table_prefix=__CLI_REPL_TABLE_PFX, **kwargs)
        ctx["cli"]["stmt"] += __cli_init_repl_table(ctx["cli"]["conn"], table_name, cols_, **kwargs)

        # Create function and trigger AFTER INSERT
        ctx["cli"]["stmt"] += __cli_create_func_after_insert(ctx["cli"]["conn"], table_name, cols_, replace=True, **kwargs)
        ctx["cli"]["stmt"] += __cli_create_trig_after_insert(ctx["cli"]["conn"], table_name, replace=True, **kwargs)

        # Create function and trigger AFTER UPDATE
        ctx["cli"]["stmt"] += __cli_create_func_after_update(ctx["cli"]["conn"], table_name, cols_, replace=True, **kwargs)
        ctx["cli"]["stmt"] += __cli_create_trig_after_update(ctx["cli"]["conn"], table_name, replace=True, **kwargs)

        # Create function and trigger BEFORE DELETE
        ctx["cli"]["stmt"] += __cli_create_func_before_delete(ctx["cli"]["conn"], table_name, cols_, replace=True, **kwargs)
        ctx["cli"]["stmt"] += __cli_create_trig_before_delete(ctx["cli"]["conn"], table_name, replace=True, **kwargs)

        # Create server repl up table
        ctx["srv"]["stmt"] += _dbu.m.db_table_ddl(
            ctx["srv"]["conn"], table_name, col_defs_ + [__REPL_COL_ID_NO_SEQ, __REPL_COL_OP, __REPL_COL_CREATE_DT],
            None, None, table_prefix=__SRV_REPL_UP_TABLE_PFX % ctx["cashreg_id"], **kwargs,
        )

        # Create server repl down table
        ctx["srv"]["stmt"] += _dbu.m.db_table_ddl(
            ctx["srv"]["conn"], table_name, col_defs_ + [__REPL_COL_ID, __REPL_COL_OP, __REPL_COL_CREATE_DT], #
            None, None, table_prefix=__SRV_REPL_DOWN_TABLE_PFX % ctx["cashreg_id"], **kwargs,
        )

        # Insert record into ffba_cashreg_repl_table
        ctx["srv"]["stmt"] += __srv_insert_repl_table_record(ctx["srv"]["conn"], ctx["cashreg_id"], table_name, **kwargs)

    tables_ = __client_nonrepl_tables(ctx["cli"]["conn"], **kwargs)
    for t_ in tables_:
        __create_table_repl_objects(ctx, t_, **kwargs)

    __apply_commit(ctx, **kwargs)


def __cli_drop_trig_after_insert(conn, table_name, **kwargs):
    ddl_ = (
        "DROP TRIGGER IF EXISTS {repl_trig_i_pfx}{table_name} ON {table_name}"
    ).format(
        table_name=table_name,
        repl_trig_i_pfx=__REPL_TRIG_I_PFX,
    )
    if kwargs.get("apply"): conn.execute(ddl_, **kwargs)
    return [ddl_, ]


def __cli_drop_trig_after_update(conn, table_name, **kwargs):
    ddl_ = (
        "DROP TRIGGER IF EXISTS {repl_trig_u_pfx}{table_name} ON {table_name}"
    ).format(
        table_name=table_name,
        repl_trig_u_pfx=__REPL_TRIG_U_PFX
    )
    if kwargs.get("apply"): conn.execute(ddl_, **kwargs)
    return [ddl_, ]


def __cli_drop_trig_before_delete(conn, table_name, **kwargs):
    ddl_ = (
        "DROP TRIGGER IF EXISTS {repl_trig_d_pfx}{table_name} ON {table_name}"
    ).format(
        table_name=table_name,
        repl_trig_d_pfx=__REPL_TRIG_D_PFX
    )
    if kwargs.get("apply"): conn.execute(ddl_, **kwargs)
    return [ddl_, ]


def __cli_drop_table_repl_triggers(conn, table_name, **kwargs):
    trigs_ = _dbu.m.db_table_triggers(conn, table_name, [
        "t.tgname in ('{i_pfx}{table_name}', '{u_pfx}{table_name}', '{d_pfx}{table_name}')".format(
            table_name=table_name,
            i_pfx=__REPL_TRIG_I_PFX,
            u_pfx=__REPL_TRIG_U_PFX,
            d_pfx=__REPL_TRIG_D_PFX,
        ),
    ], **kwargs)
    for t_ in trigs_:
        conn.execute("DROP TRIGGER %s ON %s.%s" % (t_["trig_name"], t_["schema_name"], t_["table_name"]), **kwargs)


def __cli_drop_table_repl_functions(conn, table_name, **kwargs):
    funcs_ = _dbu.m.db_functions(conn, [
        "p.proname in ('{i_pfx}{table_name}', '{u_pfx}{table_name}', '{d_pfx}{table_name}')".format(
            table_name=table_name,
            i_pfx=__REPL_FUNC_I_PFX,
            u_pfx=__REPL_FUNC_U_PFX,
            d_pfx=__REPL_FUNC_D_PFX,
        ),
    ], **kwargs)
    for f_ in funcs_:
        conn.execute("DROP FUNCTION %s(%s)" % (f_["func_name"], f_["args"]), **kwargs)


def __cli_repl_tables(conn, **kwargs):
    # Fetch repl db_tables
    tables_ = []
    for t_ in _dbu.m.db_owned_tables(conn, ["c.relname LIKE '{}%'".format(__CLI_REPL_TABLE_PFX), ], **kwargs):
        tables_.append({"table_name": t_["table_name"][len(__CLI_REPL_TABLE_PFX):], "repl_table": t_})
    return tables_


# TODO: do reload modules
def drop_repl_objects(ctx, **kwargs):

    # Retrieve SERVER ffba_cashreg_repl_table records
    # reg_tables_ = ctx["srv"]["conn"].select_all(
    #     "SELECT id, table_name FROM ffba_cashreg_repl_table WHERE cashreg_id = %s",
    #     (ctx["cashreg_id"], ), **kwargs)
    for t_ in ctx["srv"]["conn"].select_all(
        "SELECT id, table_name FROM ffba_cashreg_repl_table WHERE cashreg_id = %s",
        (ctx["cashreg_id"], ), **kwargs):

        # Drop SERVER repl up table
        ctx["srv"]["conn"].execute(
            "DROP TABLE %s" % (__SRV_REPL_UP_TABLE_PFX % ctx["cashreg_id"]) + t_[1],
            ignore_errs=(psycopg2.errorcodes.UNDEFINED_TABLE, ), **kwargs)
        # Drop SERVER repl down tables
        ctx["srv"]["conn"].execute(
            "DROP TABLE %s" % (__SRV_REPL_DOWN_TABLE_PFX % ctx["cashreg_id"]) + t_[1],
            ignore_errs=(psycopg2.errorcodes.UNDEFINED_TABLE, ), **kwargs)
        # Delete SERVER ffba_cashreg_repl_table record
        ctx["srv"]["conn"].execute("DELETE FROM ffba_cashreg_repl_table r WHERE r.id = %s", (t_[0], ), **kwargs)

    # Retrieve CLIENT existing repl tables
    # repl_tables_ = __cli_repl_tables(ctx["cli"]["conn"], **kwargs)
    for r_ in __cli_repl_tables(ctx["cli"]["conn"], **kwargs):
        # Drop CLIENT replication table
        ctx["cli"]["conn"].execute(
            "DROP TABLE %s.%s" % (r_["repl_table"]["schema_name"], r_["repl_table"]["table_name"]),
            ignore_errs=(psycopg2.errorcodes.UNDEFINED_TABLE, ), **kwargs)
        # Drop CLIENT replication triggers on the table
        __cli_drop_table_repl_triggers(ctx["cli"]["conn"], r_["table_name"], **kwargs)
        # Drop CLIENT replication functions on the table
        __cli_drop_table_repl_functions(ctx["cli"]["conn"], r_["table_name"], **kwargs)

    __apply_commit(ctx, **kwargs)


# Table list to replicate
def __srv_cashreg_repl_tables(conn, cashreg_id, **kwargs):
    repl_tables_ = []
    for t_ in conn.select_all((
        "SELECT r.id, r.table_name,"
        " coalesce(r.last_repl_id, -1),"
        " coalesce(r.last_repl_down_id, -1),"
        " coalesce(r.last_upld_srv_id, -1),"
        " d.insert_order, d.ident_cols"
        " FROM ffba_cashreg_repl_table AS r"
        " LEFT JOIN ffba_cashreg_table_def AS d"
        " ON r.table_name = d.table_name"
        " WHERE r.cashreg_id = %s"
        " ORDER BY d.insert_order"
    ), (cashreg_id, ), **kwargs):
        repl_tables_.append(
            {"id": t_[0], "table_name": t_[1],
            "last_repl_id": t_[2], "last_repl_down_id": t_[3], "last_upld_srv_id": t_[4],
            "insert_order": t_[5], "ident_cols": t_[6]},
        )
    return repl_tables_


# Apply events to table records
def __apply_op_recs(conn, table_name, columns, records, op, ident_columns, **kwargs):
    try:
        id_cols_ = re.findall(r"[\w]+", ident_columns)
    except:
        raise _t.m.msg_exc("Can't parse ident columns '%s' for table '%s'" % (ident_columns, table_name, ))
    rowcount_ = -1
    if op == __REPL_OP_NEW:
        rowcount_ = _dbu.m.db_table_insert_rows(conn, table_name, columns, records, **kwargs)
    elif op == __REPL_OP_UPDATE:
        rowcount_ = _dbu.m.db_table_update_rows(conn, table_name, columns, records, id_cols_, **kwargs)
    elif op == __REPL_OP_DELETE:
        _dbu.m.db_table_delete_rows(conn, table_name, columns, records, id_cols_, ignore_not_exist=True, **kwargs)
        rowcount_ = -2
    else:
        raise ValueError("Can't apply to '%s': wrong operation code '%s'; RECS: %s" % (table_name, op, records))
    if rowcount_ != len(records) and rowcount_ != -2:
        raise _t.m.OperationError("Not all records were applied: op=%s; count/applied=%s/%s; records=%s" % (
            op, len(records), rowcount_, records))


def srv_upload_table(ctx, cashreg, table, **kwargs):

    repl_up_tname_ = __srv_repl_up_tname(table["table_name"], cashreg["id"])
    upld_tname_ = __srv_upld_tname(table["table_name"])

    last_upld_id_ = table["last_upld_srv_id"]
    max_upld_id_ = _t.m.nvl(ctx["srv"]["conn"].select_one(
        "SELECT max(%s) FROM %s" % (__REPL_COL_ID["col_name"], repl_up_tname_), **kwargs, )[0], -1)

    if max_upld_id_ <= last_upld_id_: return

    ident_cols_ = ctx["srv"]["conn"].select_one((
        "SELECT ident_cols"
        " FROM ffba_cashreg_table_def"
        " WHERE table_name = %s"
    ), (table["table_name"], ), **kwargs)[0]

    if not re.search("siteguid", ident_cols_): ident_cols_ += ", siteguid"

    siteguid_ = None
    col_defs_ = _dbu.m.db_table_columns(ctx["srv"]["conn"], repl_up_tname_, **kwargs)
    repl_up_cols_ = _t.m.dalv(col_defs_, "col_name")
    repl_up_cols_ = filter_repl_cols(repl_up_cols_)

    upld_cols_ = copy.deepcopy(repl_up_cols_)
    if "siteguid" not in repl_up_cols_:
        upld_cols_.append("siteguid")
        siteguid_ = cashreg["siteguid"]
    repl_up_cols_.append(__REPL_COL_OP["col_name"])

    def __do_upload(ctx, cashreg, table, max_upld_id, **kwargs):

        def __limit_upld_max_id(max_id, last_id):
            __RECS_MAX = 1000
            if last_id < 0: last_id = 0
            if max_id - last_id > __RECS_MAX:
                return last_id + __RECS_MAX
            return max_id

        # Select LAST_UPLD_SRV_ID and lock table record for update
        last_id_ = ctx["srv"]["conn"].select_one((
            "SELECT coalesce(last_upld_srv_id, -1)"
            " FROM ffba_cashreg_repl_table"
            " WHERE cashreg_id = %s AND table_name = %s"
            " FOR UPDATE"
        ), (cashreg["id"], table["table_name"]), **kwargs, )[0]

        if max_upld_id <= last_id_: return

        max_id_ = __limit_upld_max_id(max_upld_id, last_id_)

        recs_ = _dbu.m.db_table_select(
            ctx["srv"]["conn"], repl_up_tname_, repl_up_cols_,
            [__REPL_COL_ID["col_name"] + " between %s and %s", ],
            [__REPL_COL_ID["col_name"], ], [last_id_ + 1, max_id_, ], **kwargs)

        # If 'siteguid' column not in REPL_UP table - add siteguid from CASHREG settings
        if siteguid_:
            i_ = 0
            for i_, r_ in enumerate(recs_):
                l_ = list(r_)
                l_.insert(-1, siteguid_)
                r_ = tuple(l_)
                recs_[i_] = r_

        op_act_ = None
        op_recs_ = []
        for r_ in recs_:
            op_ = r_[-1]
            if op_ == __REPL_OP_INIT: op_ = __REPL_OP_NEW
            if not op_act_: op_act_ = op_
            if op_act_ != op_:
                __apply_op_recs(ctx["srv"]["conn"], upld_tname_, upld_cols_, op_recs_, op_act_, ident_cols_, **kwargs)
                op_recs_ = []
            op_recs_.append(r_)
            op_act_ = op_
        __apply_op_recs(ctx["srv"]["conn"], upld_tname_, upld_cols_, op_recs_, op_act_, ident_cols_, **kwargs)

        # Update table (upload to server) record:
        # - last ID
        # - actual DT
        ctx["srv"]["conn"].execute((
            "UPDATE ffba_cashreg_repl_table"
            " SET last_upld_srv_id = %s, last_upld_srv_dt = now()"
            " WHERE id = %s"
        ), (max_id_, table["id"]), **kwargs)

        return max_id_

    while last_upld_id_ < max_upld_id_:
        last_upld_id_ = __do_upload(ctx, cashreg, table, max_upld_id_, **kwargs)
        _dbc.m.db_apply_commit(ctx, commit=True)


def srv_upload(ctx, **kwargs):
    cashregs_ = __cashreg_list(ctx["srv"]["conn"], **kwargs)
    for c_ in cashregs_:
        tables_ = __srv_cashreg_repl_tables(ctx["srv"]["conn"], c_["id"], **kwargs)
        for t_ in tables_:
            srv_upload_table(ctx, c_, t_, **kwargs)


# TODO: do reload modules
def replicate_up(ctx, **kwargs):
    tables_ = __srv_cashreg_repl_tables(ctx["srv"]["conn"], ctx["cashreg_id"], **kwargs)

    def limit_max_id(max_id, last_id):
        RECS_MAX = 1000
        if last_id < 0: last_id = 0
        if max_id - last_id > RECS_MAX:
            return last_id + RECS_MAX
        return max_id

    for t_ in tables_:
        # Retrieve and check repl up max ID for the table
        cli_repl_table_name_ = __cli_repl_table_name(t_["table_name"])
        max_repl_id_ = _t.m.nvl(ctx["cli"]["conn"].select_one(
            "SELECT max(%s) FROM %s" % (__REPL_COL_ID["col_name"], cli_repl_table_name_), **kwargs, )[0], -1)
        if max_repl_id_ <= t_["last_repl_id"]: continue

        max_repl_id_ = limit_max_id(max_repl_id_, t_["last_repl_id"])

        # if t_["table_name"] != "products": continue # (D)ebug line

        #
        col_defs_ = _dbu.m.db_table_columns(ctx["cli"]["conn"], cli_repl_table_name_, **kwargs)
        cols_ = _t.m.dalv(col_defs_, "col_name")

        # Retrieve all records to be replicated to SERVER
        recs_ = _dbu.m.db_table_select(
            ctx["cli"]["conn"], cli_repl_table_name_, cols_,
            [__REPL_COL_ID["col_name"] + " between %s and %s", ],
            [__REPL_COL_ID["col_name"], ], [t_["last_repl_id"] + 1, max_repl_id_, ], **kwargs)

        # Insert records into SERVER replication (up) table
        _dbu.m.db_table_insert_rows(ctx["srv"]["conn"], __srv_repl_up_tname(t_["table_name"], ctx["cashreg_id"]), cols_, recs_, **kwargs)

        ctx["srv"]["conn"].execute((
            "UPDATE ffba_cashreg_repl_table"
            " SET last_repl_id = %s,"
            " last_update_dt = now()"
            " WHERE id = %s"
        ), (max_repl_id_, t_["id"]), **kwargs)

        __apply_commit(ctx, **kwargs)

# Deprecated
def replicate(ctx, **kwargs):
    return replicate_up(ctx, **kwargs)


# def __srv_create_repl_down_tables(ctx, **kwargs):
#     # Retrieve table list to replicate
#     tables_ = __srv_cashreg_repl_tables(ctx["srv"]["conn"], ctx["cashreg_id"], **kwargs)
#     for t_ in tables_:
#         cols_ = _dbu.m.table_columns(ctx["cli"]["conn"], t_["table_name"], **kwargs)
#         ctx["srv"]["stmt"] += __srv_create_repl_down_table(ctx["srv"]["conn"], ctx["cashreg_id"], t_["table_name"], cols_, **kwargs)
#     __apply_commit(ctx)


def __replicate_table_down(ctx, table, **kwargs):
    # Retrieve and check repl down max ID of the table
    max_repl_down_id_ = _t.m.nvl(ctx["srv"]["conn"].select_one(
        "SELECT max(%s) FROM %s" % (
            __REPL_COL_ID["col_name"], __srv_repl_down_table_name(table["table_name"], ctx["cashreg_id"]
        )), **kwargs)[0], -1)
    if max_repl_down_id_ <= table["last_repl_down_id"]: return

    # Retrieve column definitions
    col_defs_ = _dbu.m.db_table_columns(ctx["cli"]["conn"], table["table_name"], **kwargs)
    repl_col_defs_ = col_defs_ + [{"col_name": __REPL_COL_ID["col_name"]}, {"col_name": __REPL_COL_OP["col_name"]}, ]

    # Retrieve all REPL DOWN records
    recs_ = _dbu.m.db_table_select(
        ctx["srv"]["conn"],
        __srv_repl_down_table_name(table["table_name"], ctx["cashreg_id"]), # table REPL DOWN
        _t.m.dalv(repl_col_defs_, "col_name"), # columns including REPL ID & OP
        ["%s" % (__REPL_COL_ID["col_name"], ) + " between %s and %s", ], # where REPL ID
        [__REPL_COL_ID["col_name"], ], # order by REPL ID
        (table["last_repl_down_id"] + 1, max_repl_down_id_, ), # ARGS
        **kwargs,
    )

    # Apply repl down records to client tables
    def __do_repl_down_op(conn, table_name, columns, records, op, ident_columns, **kwargs):
        id_cols_ = re.findall(r"[\w]+", ident_columns)
        rowcount_ = -1
        if op == __REPL_OP_NEW:
            rowcount_ = _dbu.m.db_table_insert_rows(conn, table_name, columns, records, **kwargs)
        elif op == __REPL_OP_UPDATE:
            rowcount_ = _dbu.m.db_table_update_rows(conn, table_name, columns, records, id_cols_, **kwargs)
        elif op == __REPL_OP_DELETE:
            _dbu.m.db_table_delete_rows(conn, table_name, columns, records, id_cols_, ignore_not_exist=True, **kwargs)
            rowcount_ = -2
        else:
            raise ValueError("Can't replicate table '%s' down: wrong operation code '%s'; RECS: %s" % (table_name, op, records))
        if rowcount_ != len(records) and rowcount_ != -2:
            raise _t.m.OperationError("Not all records were applied: records=%s, rowcount=%s" % (len(records), rowcount_))

    op_act_ = None
    op_recs_ = []
    for r_ in recs_:
        op_ = r_[-1]
        if op_ == __REPL_OP_INIT:
            op_ = __REPL_OP_NEW
        if op_act_ == None: op_act_ = op_
        if op_act_ != op_:
            __do_repl_down_op(ctx["cli"]["conn"], table["table_name"], _t.m.dalv(col_defs_, "col_name"), op_recs_, op_act_, table["ident_cols"], **kwargs)
            op_recs_ = []
        op_recs_.append(r_)
        op_act_ = op_
    __do_repl_down_op(ctx["cli"]["conn"], table["table_name"], _t.m.dalv(col_defs_, "col_name"), op_recs_, op_act_, table["ident_cols"], **kwargs)

    # Update table repl record - set last repl down ID and DT
    ctx["srv"]["conn"].execute(
        "UPDATE ffba_cashreg_repl_table SET last_repl_down_id = %s, last_repl_down_dt = now() WHERE id = %s",
        (max_repl_down_id_, table["id"], ), **kwargs)

    __apply_commit(ctx, **kwargs)


# TODO: do reload modules
def replicate_down(ctx, **kwargs):
    tabs_ = __srv_cashreg_repl_tables(ctx["srv"]["conn"], ctx["cashreg_id"], **kwargs)
    for t_ in tabs_:
        __replicate_table_down(ctx, t_, **kwargs)


# def create_products_pkey(ctx, **kwargs):
#     ctx["cli"]["conn"].execute("alter table products add constraint products_pkey primary key (id, siteguid)", **kwargs)
#     __apply_commit(ctx, **kwargs)


def alter_db_constraints(ctx, **kwargs):
    ctx["cli"]["conn"].execute("ALTER TABLE stockcurrent DROP CONSTRAINT stockcurrent_fk_1", **kwargs)
    ctx["cli"]["conn"].execute((
        "ALTER TABLE stockcurrent"
        " ADD CONSTRAINT stockcurrent_fk_1 FOREIGN KEY (product)"
        " REFERENCES products (id)"
        " ON DELETE CASCADE"
    ), **kwargs)

    ctx["cli"]["conn"].execute("ALTER TABLE ticketlines DROP CONSTRAINT ticketlines_fk_2", **kwargs)
    ctx["cli"]["conn"].execute((
        "ALTER TABLE ticketlines"
        " ADD CONSTRAINT ticketlines_fk_2 FOREIGN KEY (product)"
        " REFERENCES products (id)"
        " ON DELETE CASCADE"
    ), **kwargs)

    __apply_commit(ctx, **kwargs)


def __main():
    import db_core as db

    __SDB = {
        ...
    }

    # __CDB = {
    # }

    ctx_ = {
        # "cashreg_id": 3,
        "srv": {
            "conn": db.Db(__SDB, database_equals_user=True),
            "stmt": [],
        },
        # "cli": {
        #     "conn": db.Db(__CDB, database_equals_user=True),
        #     "stmt": [],
        # },
    }

    # cols_ = re.findall(r"[\w]+", "id,; one:, two   , three")

    # replicate_down(ctx_, 3)
    # replicate_down(ctx_, 3)

    # create_repl_objects(ctx_, 4)
    # drop_repl_objects(ctx_, 4)
    # replicate_up(ctx_, 4)
    srv_upload(ctx_, apply=True)

    # ctx_["srv"]["conn"].commit()
    # ctx_["cli"]["conn"].commit()


if __name__ == "__main__":
    __main()
