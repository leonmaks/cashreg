''' Cashreg Root 171203_2100 '''
import traceback
import logging

import mod
_t = mod.Mod("tittles")
_dbc = mod.Mod("db_core")
_rpl = mod.Mod("cashreg_repl")

def mods_rld(recursive=False):
    """Reload modules"""
    _t.reload()
    _dbc.reload()
    _rpl.reload()
    if recursive:
        _dbc.m.mods_rld(recursive)
        _rpl.m.mods_rld(recursive)


_log = logging.getLogger(__name__)


def __read_cashreg_config(ctx, **kwargs):
    try:
        rec_ = ctx["srv"]["conn"].select_one(
            "SELECT * FROM ffba_cashreg_get_config(%s)",
            (ctx["cashreg_id"], ), **kwargs)
        ctx["cli"]["parms"] = {
            "USER": rec_[0],
            "PASSWORD": rec_[1],
            "DATABASE": rec_[2],
            "HOST": rec_[3],
        }
    except Exception as e_:
        raise _t.m.ConfigError(
            "Unable to retrieve CASHREG_CONFIG from SERVER DB (cashreg_id=%s, conn=%s): %s" % (
                ctx["cashreg_id"], ctx["srv"]["conn"], e_
            ))


__CASHREG_ACTION_EXEC_MODE_ONCE = "1"
__CASHREG_ACTION_EXEC_MODE_ALWAYS = "*"

__CACHREG_ACTION_EXEC_STATUS_ACTIVE = "A"
__CACHREG_ACTION_EXEC_STATUS_DONE = "D"


def __read_action_exec(ctx, **kwargs):
    actions_ = []
    try:
        for e_ in ctx["srv"]["conn"].select_all((
            "SELECT a.id, a.name, a.command, e.id, e.mode, e.status"
            " FROM ffba_cashreg_action a, ffba_cashreg_action_exec e"
            " WHERE e.cashreg_id = %s"
            " AND e.status in (%s)"
            " AND a.id = e.cashreg_action_id"
            " ORDER BY a.call_order"
        ), (ctx["cashreg_id"], __CACHREG_ACTION_EXEC_STATUS_ACTIVE, ), **kwargs):
            actions_.append({
                "action_id": e_[0],
                "action_name": e_[1],
                "action_command": e_[2],
                "action_exec_id": e_[3],
                "action_exec_mode": e_[4],
                "action_exec_status": e_[5],
            })
    except Exception as e_:
        raise _t.m.ConfigError("Unable to read ACTION EXEC from SERVER DB (cashreg_id=%s, conn=%s): %s" % (ctx["cashreg_id"], ctx["srv"]["conn"], e_))
    return actions_


def __update_action_exec_status(action, ctx, **kwargs):
    ctx["srv"]["conn"].execute((
        "UPDATE ffba_cashreg_action_exec SET exec_dt=now()%s WHERE id=%s"
    ) % (
        action.get("action_exec_mode") == __CASHREG_ACTION_EXEC_MODE_ONCE and ", status='%s'" % __CACHREG_ACTION_EXEC_STATUS_DONE or "",
        action.get("action_exec_id"),
    ), **kwargs)
    ctx["srv"]["conn"].commit()


def run_root(ctx, **kwargs):
    """Run Root"""

    mods_rld(True)

    ctx["cli"] = {
        "conn": None,
        "parms": {},
        "stmt": [],
    }

    def __action_exec(action, ctx, **kwargs):
        # Run action
        _log.info("action=%s", action.get("action_name", "Noname"))
        try:
            _rpl.run(action.get("action_command"), ctx, **kwargs)
        except Exception:
            _log.error(traceback.format_exc())
        finally:
            # Rollback all uncommitted changes
            ctx["srv"]["conn"].rollback()
            # Update action status
            __update_action_exec_status(action, ctx, **kwargs)

    try:
        # Read Client parameters from Server DB
        __read_cashreg_config(ctx, **kwargs)
        # Initialize DB connection
        ctx["cli"]["conn"] = _dbc.m.Db(ctx["cli"]["parms"], database_equals_user=True, **kwargs)
        # Read list of actions to execute
        actions_ = __read_action_exec(ctx, **kwargs)
        _log.info("actions=%s, srv_conn=%s, cli_conn=%s", actions_ and _t.m.dalv(actions_, "action_name") or "[]", ctx["srv"]["conn"], ctx["cli"]["conn"])
        # Execute all listed actions in sequence
        for a_ in actions_: __action_exec(a_, ctx, **kwargs)

    finally:
        # Close and delete client DB conn
        if ctx["cli"]["conn"]:
            ctx["cli"]["conn"].close()
            del ctx["cli"]["conn"]
            ctx["cli"] = None
