''' Cashreg Service 171203_2100 '''
import os
import logging.config
import traceback

import servicemanager as sm
import win32event as w32e
import win32service as w32s
import win32serviceutil as w32su
from win32api import SetConsoleCtrlHandler  # pylint: disable-msg=E0611

import mod
_conf = mod.Mod("cashreg_conf")
_dbc = mod.Mod("db_core")
_act = mod.Mod("cashreg_actuator")

def _conf_rld():
    _conf.reload()

def _mods_rld(recursive=False):
    _dbc.reload()
    _act.reload()
    if recursive:
        _dbc.m.mods_rld(recursive)
        _act.m.mods_rld(recursive)


EXEC_INTERVAL = 300000
STOP_CHECK_INTERVAL = 5000

LOG_LEVEL = logging.INFO
LOG_DB_FORMAT = "%(levelname)s: %(asctime)s %(module)s:%(lineno)d(%(funcName)s) %(message)s"

logging.basicConfig(level=LOG_LEVEL, format=LOG_DB_FORMAT)
_log = logging.getLogger(__name__)


class SSLSMTPHandler(logging.handlers.SMTPHandler):
    """ SMTP logging handler
        https://stackoverflow.com/questions/36937461/how-can-i-send-an-email-using-python-loggings-smtphandler-and-ssl
    """
    def emit(self, record):
        smtp_ = None
        try:
            import smtplib
            from email.utils import formatdate
            port_ = self.mailport
            if not port_:
                port_ = smtplib.SMTP_PORT
            smtp_ = smtplib.SMTP_SSL(self.mailhost, port_)
            msg_ = self.format(record)
            msg_ = "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\n\r\n%s" % (
                self.fromaddr,
                ",".join(self.toaddrs),
                self.getSubject(record),
                formatdate(),
                msg_
            )
            if self.username:
                smtp_.login(self.username, self.password)
            smtp_.sendmail(self.fromaddr, self.toaddrs, msg_)
            smtp_.quit()
        except (KeyboardInterrupt, SystemExit):
            raise
        except: # pylint: disable-msg=W0702
            self.handleError(record)


class LogDbHandler(logging.Handler):
    """Database logging handler"""
    def __init__(self, parms, local_id):
        logging.Handler.__init__(self)
        self.__local_id = local_id
        self.__conn = _dbc.m.Db(parms, database_equals_user=True)

    def emit(self, record):
        try:
            self.__conn.execute(
                ("INSERT INTO ffba_cashreg_log"
                 " (identity, level, level_name, msg, create_dt, create_user)"
                 " VALUES (%s, %s, %s, %s, now(), current_user)"),
                (self.__local_id, record.levelno, record.levelname, record.message.strip(), ),
                commit="statement", no_logging=True)
        except Exception as e_:
            # If exception - remove log DB handler, it may cause infinite loop
            logging.getLogger().removeHandler(self)
            _log.error("Can't log to DB '%s' (logging db handler removed): %s", self.__conn, e_)
            raise

    def close(self):
        self.__conn.close()
        super(LogDbHandler, self).close()

class CashregService(w32su.ServiceFramework):
    """ Python Windows Service
        Based on article: http://www.chrisumbel.com/article/windows_services_in_python
    """
    # you can NET START/STOP the service by the following name
    _svc_name_ = "CashregService"
    # this text shows up as the service name
    # in the Service Control Manager (SCM)
    _svc_display_name_ = "Cashreg Service"
    # this text shows up as the description in the SCM
    _svc_description_ = "Cashreg service proceeding POS replication with SERVER"

    def __init__(self, args):
        w32su.ServiceFramework.__init__(self, args)
        SetConsoleCtrlHandler(lambda x: True, True)
        # create an event to listen for stop requests on
        self.hWaitStop = w32e.CreateEvent(None, 0, 0, None)
        # self.__srv_db_parms = None

    def __actuate(self):    # pylint: disable-msg=R0201

        ctx_ = {
            "local_id": None,
            "srv": {
                "conn": None,
                "stmt": [],
            }
        }

        try:
            # Reload CONFIG
            _conf_rld()

            # Set logging
            log_conf_ = getattr(_conf.m, "LOG_CONFIG", None)
            if log_conf_:
                logging.config.dictConfig(log_conf_)

            # Reload modules
            _mods_rld(True)

            # Retrieve LOCAL_ID
            ctx_["local_id"] = getattr(_conf.m, "LOCAL_ID", None)
            if not ctx_["local_id"]:
                raise Exception("LOCAL_ID not defined")

            # Retrieve SERVER DB connection parameters
            conn_parms_ = getattr(_conf.m, "SERVER_DB", None)
            if not conn_parms_:
                raise Exception("SERVER_DB not defined")
            ctx_["srv"]["conn"] = _dbc.m.Db(conn_parms_, database_equals_user=True)

            ctx_["settings"] = _conf.m

            _act.m.actuate(ctx_)

        except: # pylint: disable-msg=W0702
            try:
                _log.error(traceback.format_exc())
            # Suppress further logging exceptions
            except: pass # pylint: disable-msg=W0702, C0321

        finally:
            # Close and delete server DB conn
            if ctx_["srv"]["conn"]:
                ctx_["srv"]["conn"].close()
                del ctx_["srv"]["conn"]
            logging.shutdown()
            logging.basicConfig(level=LOG_LEVEL, format=LOG_DB_FORMAT)

    def SvcDoRun(self):
        """Core service logic"""

        # Go to module install directory
        install_dir_ = os.path.dirname(__file__)
        if install_dir_:
            os.chdir(install_dir_)

        # "Service started" message to Windows Event Log
        sm.LogMsg(                          # pylint: disable-msg=E1101
            sm.EVENTLOG_INFORMATION_TYPE,   # pylint: disable-msg=E1101
            sm.PYS_SERVICE_STARTED,         # pylint: disable-msg=E1101
            (self._svc_name_, ""))

        try:
            rc_ = None
            # if the stop event hasn't been fired keep looping
            while rc_ != w32e.WAIT_OBJECT_0:    # pylint: disable-msg=E1101

                # Actuate job
                self.__actuate()

                # Pause for EXEC_INTERVAL and listen for a stop event every STOP_CHECK_INTERVAL
                ei_ = getattr(_conf.m, "EXEC_INTERVAL", EXEC_INTERVAL)
                si_ = getattr(_conf.m, "STOP_CHECK_INTERVAL", STOP_CHECK_INTERVAL)
                ic_ = ei_ / si_
                while ic_ > 0 and rc_ != w32e.WAIT_OBJECT_0:    # pylint: disable-msg=E1101
                    ic_ -= 1
                    rc_ = w32e.WaitForSingleObject(self.hWaitStop, si_) # pylint: disable-msg=E1101

        except Exception: # pylint: disable-msg=W0703
            sm.LogErrorMsg(traceback.format_exc())  # pylint: disable-msg=E1101

    def SvcStop(self):
        """Service shut down"""
        # tell the SCM we're shutting down
        self.ReportServiceStatus(w32s.SERVICE_STOP_PENDING) # pylint: disable-msg=E1101
        # fire the stop event
        w32e.SetEvent(self.hWaitStop)   # pylint: disable-msg=E1101


if __name__ == "__main__":
    w32su.HandleCommandLine(CashregService)
