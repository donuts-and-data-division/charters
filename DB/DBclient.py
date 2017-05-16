import logging
import psycopg2
import math
#logger= logging.getLogger()


class client:
    def __init__(self, override=False):
        self.dbname="testdb"
        self.dbhost="127.0.0.1"
        self.dbport=5432
        self.dbusername="cappuser"
        self.dbpasswd="capprocks"
        self.conn == None


        if override:
            logger.info("Overriding DB connection params")
            self.dbname=DBVars.dbname
            self.dbhost=DBVars.dbhost
            self.dbport=DBVars.dbport
            self.dbusername=DBVars.dbusername
            self.dbpasswd=DBVars.dbpasswd
        pass

    # open a connection to a psql database, using the self.dbXX parameters
    def openConnection(self):
        try:
            self.conn = psycopg2.connect(dbname=self.dbname, user=self.dbusername, password=self.dbpasswd, \
                host=self.dbhost, port=self.dbport)
        except psycopg2.Error as e:
            logger.debug("Could not open connection, exception raised")
            logger.debug(e.diag.message_primary)
            raise e
        logger.debug("Opening a Connection")
        return True



if __name__=="__main__":
    import DBclient
    cl = DBclient.client()
    cl.openConnection()
    print("client connection open")
    cur = cl.conn.cursor()
    print('cursor cur is open')
    