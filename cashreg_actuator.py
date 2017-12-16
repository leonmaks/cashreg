''' Cashreg Actuator 171203_2100 '''
import logging
import sys
import os
import importlib

import mod
_t = mod.Mod("tittles")          # pylint: disable-msg=C0103

def mods_rld(recursive=False):
    """Reload modules"""
    _t.reload()
    if recursive:
        pass


install_dir = os.path.dirname(__file__)

_log = logging.getLogger(__name__)


# GET_CASHREG_ID

def __get_cashreg_id(ctx, **kwargs):
    try:
        return ctx["srv"]["conn"].select_one(
            "SELECT c.id FROM ffba_cashreg c WHERE c.identity = %s",
            (ctx["local_id"], ), reconnect_attempts=3, reconnect_timeout=1000, **kwargs)[0]
    except Exception as e_:
        raise _t.m.ConfigError("Unable to obtain CASHREG_ID (LOCAL_ID=%s) from SERVER %s: %s" % (ctx["local_id"], ctx["srv"]["conn"], e_))


# RUN_ROOT

_root_name = None


def _run_root(ctx, **kwargs):

    if not _root_name:
        raise _t.m.ConfigError("ROOT module not set for cashreg '%s'" % ctx["local_id"])

    module_name_ = os.path.splitext(_root_name)[0]
    module_ = sys.modules.get(module_name_)

    try:
        if not module_:
            module_ = importlib.import_module(module_name_)
        else:
            importlib.reload(module_)
    except Exception as e_:
        raise _t.m.ConfigError("Unable to import root module '%s': %s" % (module_name_, e_))

    module_.run_root(ctx, **kwargs)


# REFRESH_MODULES

DEPLOY_STATUS_NEW = 'N'
DEPLOY_STATUS_ACTIVE = 'A'
DEPLOY_STATUS_DELETE = 'D'

# TODO - need commit after module exec status update
def _refresh_modules(ctx, **kwargs):
    global _root_name

    # logger_ = logging.getLogger(__name__)

    reserved_module_names = []

    def _list_modules():
        # logger_ = logging.getLogger(__name__)
        modules_ = []
        for m_ in ctx["srv"]["conn"].select_all(
                "SELECT * FROM ffba_cashreg_list_modules(%s);", (ctx["local_id"], ), **kwargs):
            if m_[0] not in reserved_module_names:
                modules_.append({'name': m_[0], 'content': m_[1], 'status': m_[2], 'root': m_[3], 'id': m_[4]})
            else:
                _log.error("Module name '%s' is reserved and must NOT be used" % m_[0])
        if not modules_:
            raise _t.m.ConfigError("No import modules configured on SERVER DB for CASHREG '%s'" % ctx["local_id"])
        return modules_

    def _write_to_module(name, content):
        path_ = install_dir and os.path.join(install_dir, name) or name
        try:
            f = open(path_, 'wb')
            f.write(content.encode())
            f.close()
        except Exception as e_:
            raise _t.m.ModuleRefreshError("Can't write to module '%s': %s" % (path_, e_))

    def _remove_module(name):
        if os.path.isdir(name):
            raise _t.m.ModuleRefreshError("Can't remove module '%s': is a directory" % name)
        try:
            os.remove(name)
        except Exception as e_:
            raise _t.m.ModuleRefreshError("Can't remove module '%s': %s" % (name, e_))

    def _set_module_deploy_status(deploy_id, status):
        ctx["srv"]["conn"].execute(
            "UPDATE ffba_cashreg_module_deploy SET status = %s WHERE id = %s",
            (status, deploy_id, ), commit="statement", **kwargs)

    for m_ in _list_modules():

        if getattr(ctx["settings"], 'REWRITE_MODULES', False):
            module_exists_ = False
            module_path_ = install_dir and os.path.join(install_dir, m_['name']) or m_['name']
            if os.path.exists(module_path_):
                module_exists_ = True
                # Module already exists
                if m_['status'] == DEPLOY_STATUS_NEW or m_['status'] == DEPLOY_STATUS_DELETE:
                    _remove_module(module_path_)
                    _log.warning("Module '%s' removed, deploy status '%s'" % (module_path_, m_['status']))
            if m_['status'] == DEPLOY_STATUS_NEW or (m_['status'] == DEPLOY_STATUS_ACTIVE and not module_exists_):
                _write_to_module(module_path_, m_['content'])
                if m_['status'] == DEPLOY_STATUS_NEW:
                    _set_module_deploy_status(m_['id'], DEPLOY_STATUS_ACTIVE)
                _log.warning("Module '%s' rewritten, deploy status set to '%s'" % (module_path_, DEPLOY_STATUS_ACTIVE))

        # Set root module
        if m_['root']:
            _root_name = m_['name']


def actuate(ctx, **kwargs):

    ctx["cashreg_id"] = __get_cashreg_id(ctx, **kwargs)
    if not ctx["cashreg_id"]: raise _t.m.ConfigError("CASHREG '%s' is not registered on SERVER" % ctx["local_id"])

    _refresh_modules(ctx, **kwargs)
    _run_root(ctx, **kwargs)
