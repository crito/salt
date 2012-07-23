'''
Module to provide Postgres compatibility to salt.

In order to connect to Postgres, certain configuration is required
in /etc/salt/minion on the relevant minions. Some sample configs
might look like::

    postgres.host: 'localhost'
    postgres.port: '5432'
    postgres.pguser: 'postgres'
    postgres.pgpassword: 'Secret'
    postgres.db: 'postgres'

This data can also be passed into pillar. Options passed into opts will
overwrite options passed into pillar
'''

import logging
from salt.utils import check_or_die
from salt.exceptions import CommandNotFoundError


log = logging.getLogger(__name__)
__opts__ = {}

def __virtual__():
    '''
    Only load this module if the psql bin exists
    '''
    try:
        check_or_die('psql')
        return 'postgres'
    except CommandNotFoundError:
        return False


def version():
    '''
    Return the version of a Postgres server using the output
    from the ``psql --version`` cmd.

    CLI Example::

        salt '*' postgres.version
    '''
    version_line =  __salt__['cmd.run']('psql --version').split("\n")[0]
    name = version_line.split(" ")[1]
    ver = version_line.split(" ")[2]
    return "%s %s" % (name, ver)

def _connection_defaults(
        pguser=None,
        pgpassword=None,
        host=None,
        port=None):
    '''
    Returns a tuple of (pguser, host, port) with config, pillar, or default
    values assigned to missing values.
    '''
    if not pguser:
        pguser = __opts__.get('postgres.pguser') or __pillar__.get('postgres.pguser') or "postgres"
    if not host:
        host = __opts__.get('postgres.host') or __pillar__.get('postgres.host') or "127.0.0.1"
    if not port:
        port = __opts__.get('postgres.port') or __pillar__.get('postgres.port') or "5432"
    if not pgpassword:
        pgpassword = (__opts__.get('postgres.pgpassword')
                or __pillar__.get('postgres.pgpassword') or None)

    return (pguser, pgpassword, host, port)

'''
Database related actions
'''


def db_list(pguser=None, pgpassword=None, host=None, port=None):
    '''
    Return a list of databases of a Postgres server using the output
    from the ``psql -l`` query.

    CLI Example::

        salt '*' postgres.db_list
    '''
    (pguser, pgpassword, host, port) = _connection_defaults(
            pguser, pgpassword, host, port)

    ret = []
    env = {}
    if pgpassword:
        env['PGPASSWORD'] = pgpassword

    cmd = "psql -w -l -h {host} -U {pguser}  -p {port}".format(
            host=host, pguser=pguser, port=port)

    lines = [x for x in __salt__['cmd.run'](cmd, env=env).split("\n") if len(x.split("|")) == 6]
    header = [x.strip() for x in lines[0].split("|")]
    for line in lines[1:]:
        line = [x.strip() for x in line.split("|")]
        if not line[0] == "":
            ret.append(list(zip(header[:-1], line[:-1])))

    return ret


def db_exists(name, pguser=None, pgpassword=None, host=None, port=None):
    '''
    Checks if a database exists on the Postgres server.

    CLI Example::

        salt '*' postgres.db_exists 'dbname'
    '''
    (pguser, pgpassword, host, port) = _connection_defaults(
            pguser, pgpassword, host, port)

    databases = db_list(pguser=pguser, pgpassword=pgpassword, host=host, port=port)
    for db in databases:
        if name == dict(db).get('Name'):
            return True

    return False


def db_create(name,
              pguser=None,
              pgpassword=None,
              host=None,
              port=None,
              tablespace=None,
              encoding=None,
              local=None,
              lc_collate=None,
              lc_ctype=None,
              owner=None,
              template=None):
    '''
    Adds a databases to the Postgres server.

    CLI Example::

        salt '*' postgres.db_create 'dbname'

        salt '*' postgres.db_create 'dbname' template=template_postgis

    '''
    (pguser, pgpassword, host, port) = _connection_defaults(pguser, pgpassword, host, port)

    # check if db exists
    if db_exists(name, pguser, pgpassword, host, port):
        log.info("DB '{0}' already exists".format(name,))
        return False

    env = {}

    if pgpassword:
        env['PGPASSWORD'] = pgpassword

    cmd = 'createdb -w -h {host} -U {pguser} -p {port} {name}'.format(
        pguser=pguser, host=host, port=port, name=name)

    if tablespace:
        cmd = "{0} -D {1}".format(cmd, tablespace)

    if encoding:
        cmd = "{0} -E {1}".format(cmd, encoding)

    if local:
        cmd = "{0} -l {1}".format(cmd, local)

    if lc_collate:
        cmd = "{0} --lc-collate {1}".format(cmd, lc_collate)

    if lc_ctype:
        cmd = "{0} --lc-ctype {1}".format(cmd, lc_ctype)

    if owner:
        cmd = "{0} -O {1}".format(cmd, owner)

    if template:
        if db_exists(template, pguser, pgpassword, host, port):
            cmd = "{cmd} -T {template}".format(cmd=cmd, template=template)
        else:
            log.info("template '{0}' does not exist.".format(template, ))
            return False

    __salt__['cmd.run'](cmd, env=env)

    if db_exists(name, pguser, pgpassword, host, port):
        return True
    else:
        log.info("Failed to create DB '{0}'".format(name,))
        return False


def db_remove(name, pguser=None, pgpassword=None, host=None, port=None):
    '''
    Removes a databases from the Postgres server.

    CLI Example::

        salt '*' postgres.db_remove 'dbname'
    '''
    (pguser, pgpassword, host, port) = _connection_defaults(
            pguser, pgpassword, host, port)

    # check if db exists
    if not db_exists(name, pguser, pgpassword, host, port):
        log.info("DB '{0}' does not exist".format(name,))
        return False

    # db doesnt exist, proceed
    env = {}

    if pgpassword:
        env['PGPASSWORD'] = pgpassword

    cmd = 'dropdb -w -h {host} -U {pguser} -p {port} {name}'.format(
        pguser=pguser, host=host, port=port, name=name)

    __salt__['cmd.run'](cmd, env=env)
    if not db_exists(name, pguser, pgpassword, host, port):
        return True
    else:
        log.info("Failed to delete DB '{0}'.".format(name, ))
        return False

'''
User related actions
'''

def user_list(pguser=None, pgpassword=None, host=None, port=None):
    '''
    Return a list of users of a Postgres server.

    CLI Example::

        salt '*' postgres.user_list
    '''
    (pguser, pgpassword, host, port) = _connection_defaults(
            pguser, pgpassword, host, port)

    ret = []
    env = {}
    if pgpassword:
        env['PGPASSWORD'] = pgpassword

    cmd = "psql -w -h {host} -U {pguser} -p {port} -P pager postgres -c \"SELECT * FROM pg_roles\"".format(
        host=host, pguser=pguser, port=port)

    lines = [x for x in __salt__['cmd.run'](cmd, env=env).split("\n") if len(x.split("|")) == 13]
    header = [x.strip() for x in lines[0].split("|")]
    for line in lines[1:]:
        line = [x.strip() for x in line.split("|")]
        if not line[0] == "":
            ret.append(list(zip(header[:-1], line[:-1])))

    return ret

def user_exists(name, pguser=None, pgpassword=None, host=None, port=None):
    '''
    Checks if a user exists on the Postgres server.

    CLI Example::

        salt '*' postgres.user_exists 'username'
    '''
    (pguser, pgpassword, host, port) = _connection_defaults(
            pguser, pgpassword, host, port)

    users = user_list(pguser=pguser, pgpassword=pgpassword, host=host, port=port)
    for user in users:
        if name == dict(user).get('rolname'):
            return True

    return False

def user_create(username,
                pguser=None,
                pgpassword=None,
                host=None,
                port=None,
                createdb=False,
                createuser=False,
                encrypted=False,
                password=None):
    '''
    Creates a Postgres user.

    CLI Examples::

        salt '*' postgres.user_create 'username' pguser='pguser' host='hostname' port='port' password='password'
    '''
    (pguser, pgpassword, host, port) = _connection_defaults(
            pguser, pgpassword, host, port)

    # check if user exists
    if user_exists(username, pguser, pgpassword, host, port):
        log.info("User '{0}' already exists".format(username,))
        return False

    sub_cmd = "CREATE USER {0} WITH".format(username, )
    if password:
        sub_cmd = "{0} PASSWORD '{1}'".format(sub_cmd, password)
    if createdb:
        sub_cmd = "{0} CREATEDB".format(sub_cmd, )
    if createuser:
        sub_cmd = "{0} CREATEUSER".format(sub_cmd, )
    if encrypted:
        sub_cmd = "{0} ENCRYPTED".format(sub_cmd, )

    if sub_cmd.endswith("WITH"):
        sub_cmd = sub_cmd.replace(" WITH", "")

    env = {}
    if pgpassword:
        env['PGPASSWORD'] = pgpassword

    cmd = 'psql -w -h {host} -U {pguser} -p {port} -c "{sub_cmd}"'.format(
        host=host, pguser=pguser, port=port, sub_cmd=sub_cmd)
    return __salt__['cmd.run'](cmd, env=env)

def user_update(username,
                pguser=None,
                pgpassword=None,
                host=None,
                port=None,
                createdb=False,
                createuser=False,
                encrypted=False,
                password=None):
    '''
    Creates a Postgres user.

    CLI Examples::

        salt '*' postgres.user_create 'username' pguser='pguser' host='hostname' port='port' password='password'
    '''
    (pguser, pgpassword, host, port) = _connection_defaults(
            pguser, pgpassword, host, port)

    # check if user exists
    if not user_exists(username, pguser, host, port):
        log.info("User '{0}' does not exist".format(username,))
        return False

    sub_cmd = "ALTER USER {0} WITH".format(username, )
    if password:
        sub_cmd = "{0} PASSWORD '{1}'".format(sub_cmd, password)
    if createdb:
        sub_cmd = "{0} CREATEDB".format(sub_cmd, )
    if createuser:
        sub_cmd = "{0} CREATEUSER".format(sub_cmd, )
    if encrypted:
        sub_cmd = "{0} ENCRYPTED".format(sub_cmd, )

    if sub_cmd.endswith("WITH"):
        sub_cmd = sub_cmd.replace(" WITH", "")

    env = {}
    if pgpassword:
        env['PGPASSWORD'] = pgpassword

    cmd = 'psql -w -h {host} -U {pguser} -p {port} -c "{sub_cmd}"'.format(
        host=host, pguser=pguser, port=port, sub_cmd=sub_cmd)
    return __salt__['cmd.run'](cmd, env=env)

def user_remove(username, pguser=None, pgpassword=None, host=None, port=None):
    '''
    Removes a user from the Postgres server.

    CLI Example::

        salt '*' postgres.user_remove 'username'
    '''
    (pguser, pgpassword, host, port) = _connection_defaults(
            pguser, pgpassword, host, port)

    # check if user exists
    if not user_exists(username, pguser, host, port):
        log.info("User '{0}' does not exist".format(username,))
        return False

    # user exists, proceed
    env = {}
    if pgpassword:
        env['PGPASSWORD'] = pgpassword

    cmd = 'dropuser -w -h {host} -U {pguser} -p {port} {username}'.format(
        pguser=pguser, host=host, port=port, username=username)
    __salt__['cmd.run'](cmd, env=env)
    if not user_exists(username, pguser, host, port):
        return True
    else:
        log.info("Failed to delete user '{0}'.".format(username, ))
        return False
