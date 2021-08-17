from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        # TABLES:
        conn = Connector.DBConnector()
        sqlCreateQueries = sql.SQL("CREATE TABLE Queries("
                                   "queryID INTEGER PRIMARY KEY,"
                                   "purpose TEXT NOT NULL,"
                                   "querySize INTEGER NOT NULL,"
                                   "CHECK(queryID>0),"
                                   "CHECK(querySize>=0))")

        sqlCreateDisks = sql.SQL("CREATE TABLE Disks("
                                 "diskID INTEGER PRIMARY KEY,"
                                 "diskCompany TEXT NOT NULL,"
                                 "speed INTEGER NOT NULL,"
                                 "freeSpace INTEGER NOT NULL,"
                                 "costPerByte INTEGER NOT NULL,"
                                 "CHECK(diskID>0),"
                                 "CHECK(speed>0),"
                                 "CHECK(freeSpace>=0),"
                                 "CHECK(costPerByte>0))")

        sqlCreateRAMs = sql.SQL("CREATE TABLE RAMs("
                                "ramID INTEGER PRIMARY KEY,"
                                "ramSize INTEGER NOT NULL,"
                                "ramCompany TEXT NOT NULL,"
                                "CHECK(ramID>0),"
                                "CHECK(ramSize>0))")

        sqlCreateQueryOnDisk = sql.SQL("CREATE TABLE QueryOnDisk("
                                       "queryID INTEGER,"
                                       "diskID INTEGER,"
                                       "PRIMARY KEY(queryID, diskID),"
                                       "FOREIGN KEY(queryID) REFERENCES Queries(queryID) ON DELETE CASCADE,"
                                       "FOREIGN KEY(diskID) REFERENCES Disks(diskID) ON DELETE CASCADE)")

        sqlCreateRAMOnDisk = sql.SQL("CREATE TABLE RAMOnDisk("
                                     "ramID INTEGER,"
                                     "diskID INTEGER,"
                                     "PRIMARY KEY(ramID, diskID),"
                                     "FOREIGN KEY(ramID) REFERENCES RAMs(ramID) ON DELETE CASCADE,"
                                     "FOREIGN KEY(diskID) REFERENCES Disks(diskID) ON DELETE CASCADE)")

        # VIEWS:
        sqlCreateRunningQueriesView = sql.SQL("CREATE VIEW RunningQueries AS "
                                              "SELECT Q.queryID, querySize, purpose, D.diskID, costPerByte "
                                              "FROM Queries Q, QueryOnDisk QD, Disks D "
                                              "WHERE Q.queryID = QD.queryID AND QD.diskID = D.diskID")

        sqlCreateRunningRAMsView = sql.SQL("CREATE VIEW RunningRAMs AS "
                                           "SELECT R.ramID, R.ramCompany, D.diskID, D.diskCompany "
                                           "FROM Rams R, RAMOnDisk RD, Disks D "
                                           "WHERE R.ramID = RD.ramID AND RD.diskID = D.diskID")

        sqlRunableQueriesView = sql.SQL("CREATE VIEW RunableQueries AS "
                                        "SELECT D.diskID, Q.queryID, Q.querySize "
                                        "FROM Queries Q, Disks D "
                                        "WHERE Q.querySize <= D.freeSpace ")

        sqlTotalRAMView = sql.SQL("CREATE VIEW TotalRAM AS "
                                  "SELECT D.diskID, (SELECT COALESCE(SUM(R.ramSize), 0) FROM RAMOnDisk RD, RAMs R WHERE R.ramID = RD.ramID AND RD.diskID = D.diskID) AS totalRAM "
                                  "FROM Disks D ")

        sqlMutualDisksView = sql.SQL("CREATE VIEW MutualDisks AS "
                                     "SELECT Q1.queryID AS queryID1, Q2.queryID AS queryID2, (SELECT COUNT(*) FROM QueryOnDisk QD1, QueryOnDisk QD2 "
                                     "WHERE QD1.queryID = Q1.queryID AND QD2.queryID = Q2.queryID AND QD1.diskID = QD2.diskID) AS disksNum "
                                     "FROM Queries Q1, Queries Q2")

        transaction = createTransaction([sqlCreateQueries, sqlCreateDisks, sqlCreateRAMs,
                                         sqlCreateQueryOnDisk, sqlCreateRAMOnDisk, sqlCreateRunningQueriesView,
                                         sqlCreateRunningRAMsView, sqlRunableQueriesView, sqlTotalRAMView,
                                         sqlMutualDisksView])
        conn.execute(transaction)
        conn.commit()
    finally:
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        sqlClearQueries = sql.SQL("DELETE FROM Queries CASCADE")
        sqlClearDisks = sql.SQL("DELETE FROM Disks CASCADE")
        sqlClearRAMs = sql.SQL("DELETE FROM RAMs CASCADE")
        sqlClearQueryOnDisk = sql.SQL("DELETE FROM QueryOnDisk CASCADE")
        sqlClearRAMOnDisk = sql.SQL("DELETE FROM RAMOnDisk CASCADE")
        transaction = createTransaction([sqlClearQueries, sqlClearDisks, sqlClearRAMs,
                                         sqlClearQueryOnDisk, sqlClearRAMOnDisk])
        conn.execute(transaction)
        conn.commit()
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        # TABLES:
        sqlDropQueries = sql.SQL("DROP TABLE IF EXISTS Queries CASCADE")
        sqlDropDisks = sql.SQL("DROP TABLE IF EXISTS Disks CASCADE")
        sqlDropRAMs = sql.SQL("DROP TABLE IF EXISTS RAMs CASCADE")
        sqlDropQueryOnDisk = sql.SQL("DROP TABLE IF EXISTS QueryOnDisk CASCADE")
        sqlDropRAMOnDisk = sql.SQL("DROP TABLE IF EXISTS RAMOnDisk CASCADE")
        # VIEWS:
        sqlDropRunningQueriesView = sql.SQL("DROP TABLE IF EXISTS RunningQueries CASCADE")
        sqlDropTotalRAMView = sql.SQL("DROP TABLE IF EXISTS TotalRAM CASCADE")
        sqlDropRunningRAMsView = sql.SQL("DROP TABLE IF EXISTS RunningRAMs CASCADE")
        sqlDropRunableQueriesView = sql.SQL("DROP TABLE IF EXISTS RunableQueries CASCADE")
        sqlDropMutualDisksView = sql.SQL("DROP TABLE IF EXISTS MutualDisks CASCADE")

        transaction = createTransaction([sqlDropQueries, sqlDropDisks, sqlDropRAMs,
                                         sqlDropQueryOnDisk, sqlDropRAMOnDisk, sqlDropRunningQueriesView,
                                         sqlDropTotalRAMView,
                                         sqlDropRunningRAMsView, sqlDropRunableQueriesView, sqlDropMutualDisksView])
        conn.execute(transaction)
        conn.commit()
    finally:
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    queryID = query.getQueryID()
    purpose = query.getPurpose()
    querySize = query.getSize()
    retValue = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL(
            "INSERT INTO Queries(queryID, purpose, querySize) VALUES({queryID}, {purpose}, {querySize});") \
            .format(queryID=sql.Literal(queryID), purpose=sql.Literal(purpose), querySize=sql.Literal(querySize))

        rows_affected, _ = conn.execute(sqlQuery)
        conn.commit()
        retValue = ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION as e:
        retValue = ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        retValue = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        retValue = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        retValue = ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid as e:
        retValue = ReturnValue.ERROR

    except Exception as e:
        retValue = ReturnValue.ERROR
    finally:
        conn.close()
        return retValue


def getQueryProfile(queryID: int) -> Query:
    conn = None
    query = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL(f"SELECT * FROM Queries WHERE queryID = {queryID}").format(queryID=sql.Literal(queryID))
        rows_affected, result = conn.execute(sqlQuery)
        conn.commit()
        query = queryFromResult(result)
    except Exception as e:
        query = Query.badQuery()
    finally:
        conn.close()
        return query


def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    retValue = None
    queryID = query.getQueryID()
    querySize = query.getSize()
    try:
        conn = Connector.DBConnector()
        queryUpdateSql = sql.SQL("UPDATE Disks SET freeSpace = freeSpace + {querySize} WHERE diskID IN "
                                 "(SELECT diskID FROM QueryOnDisk WHERE queryID = {queryID}) ") \
            .format(querySize=sql.Literal(querySize), queryID=sql.Literal(queryID))

        queryDeleteSql = sql.SQL("DELETE FROM Queries WHERE QueryID={0} ").format(sql.Literal(queryID))

        sqlQuery = createTransaction([queryUpdateSql, queryDeleteSql])
        rows_effected, _ = conn.execute(sqlQuery)
        retValue = ReturnValue.OK
        conn.commit()
    except Exception as e:
        retValue = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return retValue


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    retValue = None
    diskID = disk.getDiskID()
    diskCompany = disk.getCompany()
    speed = disk.getSpeed()
    freeSpace = disk.getFreeSpace()
    costPerByte = disk.getCost()
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("INSERT INTO Disks(diskID, diskCompany, speed, freeSpace, costPerByte)"
                           "VALUES({diskID}, {diskCompany}, {speed}, {freeSpace}, {costPerByte})") \
            .format(diskID=sql.Literal(diskID), diskCompany=sql.Literal(diskCompany), speed=sql.Literal(speed),
                    freeSpace=sql.Literal(freeSpace), costPerByte=sql.Literal(costPerByte))

        rows_affected, _ = conn.execute(sqlQuery)
        conn.commit()
        retValue = ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION as e:
        retValue = ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        retValue = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        retValue = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        retValue = ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid as e:
        retValue = ReturnValue.ERROR

    except Exception as e:
        retValue = ReturnValue.ERROR
    finally:
        conn.close()
        return retValue


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    disk = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL(
            f"SELECT diskID, diskCompany, speed, freeSpace, costPerByte  FROM Disks WHERE diskID = {diskID}").format(
            queryID=sql.Literal(diskID))
        rows_affected, result = conn.execute(sqlQuery)
        conn.commit()
        disk = diskFromResult(result)
    except Exception as e:
        disk = Disk.badDisk()
    finally:
        conn.close()
        return disk


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    retValue = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("DELETE FROM Disks WHERE diskID={0}").format(sql.Literal(diskID))
        rows_effected, _ = conn.execute(sqlQuery)
        conn.commit()
        if rows_effected == 0:
            retValue = ReturnValue.NOT_EXISTS
        else:
            retValue = ReturnValue.OK
    except Exception as e:
        retValue = ReturnValue.ERROR
    finally:
        conn.close()
        return retValue


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    retValue = None
    ramID = ram.getRamID()
    ramCompany = ram.getCompany()
    ramSize = ram.getSize()
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("INSERT INTO RAMs(ramID, ramCompany, ramSize)"
                           "VALUES({ramID}, {ramCompany}, {ramSize})") \
            .format(ramID=sql.Literal(ramID), ramCompany=sql.Literal(ramCompany), ramSize=sql.Literal(ramSize))

        rows_affected, _ = conn.execute(sqlQuery)
        conn.commit()
        retValue = ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION as e:
        retValue = ReturnValue.BAD_PARAMS
    except DatabaseException.NOT_NULL_VIOLATION as e:
        retValue = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        retValue = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        retValue = ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid as e:
        retValue = ReturnValue.ERROR
    except Exception as e:
        retValue = ReturnValue.ERROR
    finally:
        conn.close()
        return retValue


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    ram = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL(f"SELECT * FROM RAMs WHERE ramID = {ramID}").format(queryID=sql.Literal(ramID))
        rows_affected, result = conn.execute(sqlQuery)
        conn.commit()
        ram = ramFromResult(result)
    except Exception as e:
        ram = RAM.badRAM()
    finally:
        conn.close()
        return ram


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    retValue = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("DELETE FROM RAMs WHERE ramID={0}").format(sql.Literal(ramID))
        rows_effected, _ = conn.execute(sqlQuery)
        conn.commit()
        if rows_effected == 0:
            retValue = ReturnValue.NOT_EXISTS
        else:
            retValue = ReturnValue.OK
    except Exception as e:
        retValue = ReturnValue.ERROR
    finally:
        conn.close()
        return retValue


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    conn = None
    retValue = None
    queryID = query.getQueryID()
    purpose = query.getPurpose()
    querySize = query.getSize()
    diskID = disk.getDiskID()
    diskCompany = disk.getCompany()
    speed = disk.getSpeed()
    freeSpace = disk.getFreeSpace()
    costPerByte = disk.getCost()
    try:
        conn = Connector.DBConnector()
        disksInsertQuery = sql.SQL("INSERT INTO Disks(diskID, diskCompany, speed, freeSpace, costPerByte)"
                                   "VALUES({diskID}, {diskCompany}, {speed}, {freeSpace}, {costPerByte})") \
            .format(diskID=sql.Literal(diskID), diskCompany=sql.Literal(diskCompany), speed=sql.Literal(speed),
                    freeSpace=sql.Literal(freeSpace), costPerByte=sql.Literal(costPerByte))

        queriesInsertQuery = sql.SQL(
            "INSERT INTO Queries(queryID, purpose, querySize) VALUES({queryID}, {purpose}, {querySize});") \
            .format(queryID=sql.Literal(queryID), purpose=sql.Literal(purpose), querySize=sql.Literal(querySize))

        transaction = createTransaction([disksInsertQuery, queriesInsertQuery])
        rows_effected, _ = conn.execute(transaction)
        conn.commit()
        retValue = ReturnValue.OK
    except DatabaseException.CHECK_VIOLATION as e:
        retValue = ReturnValue.BAD_PARAMS
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        retValue = ReturnValue.ALREADY_EXISTS
        conn.rollback()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        retValue = ReturnValue.ERROR
        conn.rollback()
    except DatabaseException.ConnectionInvalid as e:
        retValue = ReturnValue.ERROR
        conn.rollback()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        retValue = ReturnValue.ERROR
        conn.rollback()
    except Exception as e:
        retValue = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return retValue


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    queryID = query.getQueryID()
    querySize = query.getSize()
    retValue = None
    try:
        conn = Connector.DBConnector()
        sqlUpdateQuery = sql.SQL("UPDATE Disks SET freeSpace = freeSpace - {querySize}"
                                 " WHERE diskID={diskID}") \
            .format(diskID=sql.Literal(diskID), querySize=sql.Literal(querySize))

        sqlInsertQuery = sql.SQL("INSERT INTO QueryOnDisk(queryID, diskID) VALUES("
                                 "{queryID}, {diskID})").format(queryID=sql.Literal(queryID),
                                                                diskID=sql.Literal(diskID))

        transaction = createTransaction([sqlInsertQuery, sqlUpdateQuery])
        conn.execute(transaction)
        retValue = ReturnValue.OK
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        retValue = ReturnValue.NOT_EXISTS
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION:
        retValue = ReturnValue.ALREADY_EXISTS
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION:
        retValue = ReturnValue.BAD_PARAMS
        conn.rollback()
    except Exception as e:
        retValue = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return retValue


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    conn = None
    retValue = None
    queryID = query.getQueryID()
    querySize = query.getSize()
    try:
        conn = Connector.DBConnector()
        sqlUpdateQuery = sql.SQL("UPDATE Disks SET freeSpace = freeSpace + {querySize} WHERE diskID IN "
                                 "(SELECT diskID FROM QueryOnDisk WHERE diskID={diskID} AND queryID={queryID})") \
            .format(diskID=sql.Literal(diskID), queryID=sql.Literal(queryID), querySize=sql.Literal(querySize))

        sqlDeleteQuery = sql.SQL("DELETE FROM QueryOnDisk WHERE queryID = {queryID} AND diskID = {diskID}") \
            .format(queryID=sql.Literal(queryID), diskID=sql.Literal(diskID))

        transaction = createTransaction([sqlUpdateQuery, sqlDeleteQuery])
        conn.execute(transaction)
        conn.commit()
        retValue = ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        retValue = ReturnValue.OK
        conn.commit()
    except Exception:
        retValue = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return retValue


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    retValue = None
    try:
        conn = Connector.DBConnector()
        sqlInsertRAM = sql.SQL("INSERT INTO RAMOnDisk(ramID, diskID) VALUES("
                               "{ramID}, {diskID})").format(ramID=sql.Literal(ramID),
                                                            diskID=sql.Literal(diskID))

        transaction = createTransaction([sqlInsertRAM])
        conn.execute(transaction)
        conn.commit()
        retValue = ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        retValue = ReturnValue.NOT_EXISTS
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION:
        retValue = ReturnValue.ALREADY_EXISTS
        conn.rollback()
    except DatabaseException.CHECK_VIOLATION:
        retValue = ReturnValue.BAD_PARAMS
        conn.rollback()
    except Exception as e:
        retValue = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return retValue


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    retValue = None
    try:
        conn = Connector.DBConnector()

        sqlDeleteQuery = sql.SQL("DELETE FROM RAMOnDisk WHERE ramID = {ramID} AND diskID = {diskID}") \
            .format(ramID=sql.Literal(ramID), diskID=sql.Literal(diskID))

        rows_affected, _ = conn.execute(sqlDeleteQuery)
        conn.commit()
        if rows_affected == 0:
            retValue = ReturnValue.NOT_EXISTS
        else:
            retValue = ReturnValue.OK
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        retValue = ReturnValue.OK
        conn.commit()
    except Exception:
        retValue = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return retValue


def averageSizeQueriesOnDisk(diskID: int) -> float:
    conn = None
    averageSize = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("SELECT AVG(querySize) FROM RunningQueries "
                           "WHERE diskID={diskID}").format(diskID=sql.Literal(diskID))

        rows_affected, result = conn.execute(sqlQuery)
        conn.commit()
        if result[0]['avg'] is None:
            averageSize = 0
        else:
            averageSize = result[0]['avg']
    except Exception as e:
        averageSize = -1
    finally:
        conn.close()
        return averageSize


def diskTotalRAM(diskID: int) -> int:
    conn = None
    totalRAM = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("SELECT totalRAM FROM TotalRAM "
                           "WHERE diskID={diskID}").format(diskID=sql.Literal(diskID))

        rows_affected, result = conn.execute(sqlQuery)
        conn.commit()
        if result.isEmpty() or (result[0]['totalRAM'] is None):
            totalRAM = 0
        else:
            totalRAM = result[0]['totalRAM']
    except Exception as e:
        totalRAM = -1
    finally:
        conn.close()
        return totalRAM


def getCostForPurpose(purpose: str) -> int:
    conn = None
    cost = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("SELECT SUM(costPerByte*querySize) FROM RunningQueries "
                           "WHERE purpose = {purpose}").format(purpose=sql.Literal(purpose))

        rows_affected, result = conn.execute(sqlQuery)
        conn.commit()
        if result[0]['sum'] is None:
            cost = 0
        else:
            cost = result[0]['sum']
    except Exception:
        cost = -1
    finally:
        conn.close()
        return cost


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    list = []
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("SELECT queryID FROM RunableQueries "
                           "WHERE diskId={diskID} "
                           "ORDER BY queryID DESC "
                           "LIMIT 5").format(diskID=sql.Literal(diskID))

        rows_affected, result = conn.execute(sqlQuery)
        list = [result.__getitem__(i)['queryID'] for i in range(result.size())]
        conn.commit()
    finally:
        conn.close()
        return list


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    list = []
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("SELECT queryID FROM RunableQueries RQ "
                           "WHERE RQ.diskId={diskID} AND (SELECT totalRAM FROM TotalRAM TR "
                           "WHERE TR.diskID = {diskID}) - RQ.querySize >= 0 "
                           "ORDER BY queryID ASC "
                           "LIMIT 5").format(diskID=sql.Literal(diskID))

        rows_affected, result = conn.execute(sqlQuery)
        list = [result.__getitem__(i)['queryID'] for i in range(result.size())]
        conn.commit()
    finally:
        conn.close()
        return list


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    isExclusive = None
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("SELECT diskCompany FROM Disks WHERE diskID={diskID} UNION SELECT ramCompany FROM RunningRAMs "
                           "WHERE diskId={diskID} ").format(diskID=sql.Literal(diskID))
        rows_affected, result = conn.execute(sqlQuery)
        conn.commit()
        if result.size() == 1:
            isExclusive = True
        else:
            isExclusive = False
    except Exception as e:
        isExclusive = False
    finally:
        conn.close()
        return isExclusive


def getConflictingDisks() -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("SELECT DISTINCT L.diskID FROM QueryOnDisk R, QueryOnDisk L "
                           "WHERE L.queryId=R.queryId AND L.diskID <> R.diskID "
                           "ORDER BY L.diskID ASC ").format()
        rows_affected, result = conn.execute(sqlQuery)
        res = [result.__getitem__(i)['diskID'] for i in range(result.size())]
        conn.commit()
    finally:
        conn.close()
        return res


def mostAvailableDisks() -> List[int]:
    conn = None
    res = []
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("SELECT D.diskID, D.speed, (SELECT COUNT(*) FROM RunableQueries RQ "
                           "WHERE D.diskID = RQ.diskID) AS count "
                           "FROM Disks D "
                           "ORDER BY count DESC, D.speed DESC, D.diskID ASC "
                           "LIMIT 5 ").format()

        rows_affected, result = conn.execute(sqlQuery)
        res = [result.__getitem__(i)['diskID'] for i in range(result.size())]
        conn.commit()
    finally:
        conn.close()
        return res


def getCloseQueries(queryID: int) -> List[int]:
    conn = None
    list = []
    try:
        conn = Connector.DBConnector()
        sqlQuery = sql.SQL("SELECT queryID1 FROM MutualDisks WHERE queryID1 <> {queryID} AND queryID2 = {queryID} "
                           "AND disksNum >= (SELECT 0.5*disksNum From MutualDisks WHERE queryID1 = {queryID} "
                           "AND queryID2 = {queryID})"
                           "ORDER BY queryID1 "
                           "LIMIT 10").format(queryID=sql.Literal(queryID))

        rows_affected, res = conn.execute(sqlQuery)
        list = [res.__getitem__(i)['queryID1'] for i in range(res.size())]
        conn.commit()
    finally:
        conn.close()
        return list


def queryFromResult(result: Connector.ResultSet) -> Query:
    if not result.isEmpty():
        retQuery = Query(result[0]['queryID'], result[0]['purpose'], result[0]['querySize'])
    else:
        retQuery = Query.badQuery()
    return retQuery


def diskFromResult(result: Connector.ResultSet) -> Disk:
    if not result.isEmpty():
        retDisk = Disk(result[0]['diskID'], result[0]['diskCompany'], result[0]['speed'], result[0]['freeSpace'],
                       result[0]['costPerByte'])
    else:
        retDisk = Disk.badDisk()
    return retDisk


def ramFromResult(result: Connector.ResultSet) -> RAM:
    if not result.isEmpty():
        retQuery = RAM(result[0]['ramID'], result[0]['ramCompany'], result[0]['ramSize'])
    else:
        retQuery = RAM.badRAM()
    return retQuery


def createTransaction(sqlList):
    sqlList.insert(0, sql.SQL("BEGIN"))
    sqlList.append(sql.SQL("COMMIT"))
    return sql.SQL('; ').join(sqlList)
