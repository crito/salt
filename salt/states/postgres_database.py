'''
Management of PostgreSQL databases (schemas).
=============================================

The postgres_database module is used to create and manage Postgres databases.
Databases can be set as either absent or present

.. code-block:: yaml

    frank:
      postgres_database.present
'''


def present(
        name,
        owner=None,
        user=None,
        pgpassword=None,
        host=None,
        port=None):
    '''
    Ensure that the named database is present with the specified properties

    name
        The name of the database to manage

    owner
        The username of the database owner

    user
        Use user for this connection

    pgpassword
        User pgpassword for authenticating user

    host
        The host of the database server

    port
        The port of the database server
    '''
    ret = {'name': name,
           'changes': {},
           'result': True,
           'comment': 'Database {0} is already present'.format(name)}

    # check if database exists
    if __salt__['postgres.db_exists'](name, user=user, pgpassword=pgpassword,
            host=host, port=port):
        return ret

    # The database is not present, make it!
    if __opts__['test']:
        ret['result'] = None
        ret['comment'] = 'Database {0} is set to be created'.format(name)
        return ret
    if __salt__['postgres.db_create'](name, owner=owner, user=user,
            pgpassword=pgpassword, host=host, port=port):
        ret['comment'] = 'The database {0} has been created'.format(name)
        ret['changes'][name] = 'Present'
    else:
        ret['comment'] = 'Failed to create database {0}'.format(name)
        ret['result'] = False

    return ret


def absent(name, user=None, pgpassword=None, host=None, port=None):
    '''
    Ensure that the named database is absent

    name
        The name of the database to remove

    user
        Use user for this connection

    pgpassword
        User pgpassword for authenticating user

    host
        The host of the database server

    port
        The port of the database server
    '''
    ret = {'name': name,
           'changes': {},
           'result': True,
           'comment': ''}

    #check if db exists and remove it
    if __salt__['postgres.db_exists'](name, user=user, pgpassword=pgpassword,
            host=host, port=port):
        if __opts__['test']:
            ret['result'] = None
            ret['comment'] = 'Database {0} is set to be removed'.format(name)
            return ret
        if __salt__['postgres.db_remove'](name, user=user,
                pgpassword=pgpassword, host=host, port=port):
            ret['comment'] = 'Database {0} has been removed'.format(name)
            ret['changes'][name] = 'Absent'
            return ret

    # fallback
    ret['comment'] = (
            'Database {0} is not present, so it cannot be removed'
            ).format(name)
    return ret
