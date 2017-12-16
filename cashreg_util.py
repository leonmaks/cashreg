"""Cashreg Repl 170928_1920"""
import logging.config

# import tittles as t
import db_core as dbc
import db_util as dbu


if __name__ == '__main__':
    logging.basicConfig(format="%(levelname)s: %(asctime)s %(module)s:%(lineno)d(%(funcName)s) %(message)s", level=logging.DEBUG)
#     import cashreg_conf
#     logging.config.dictConfig(cashreg_conf.LOG_CONFIG)

_log = logging.getLogger(__name__)



def __main():

    __SDB = {
        ...
    }

    # ctx_ = {
    #     "cashreg_id": 3,
    #     'srv': {
    #         'conn': db.Db(__SDB, database_equals_user=True),
    #         'stmt': [],
    #     },
    # }

    sconn_ = dbc.Db(__SDB)
    sconn_.connect(debug="statement")

    _log.debug("ok")


if __name__ == "__main__":
    __main()
