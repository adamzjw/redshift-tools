__author__ = 'junwez'
 
import re
 
def pivotSQLGenerate(cursor, tableName, rowBy, columnBy, valueNames, groupFuncName="SUM", isTemp = True, prefix = None, columnFilter = None):
    """ Generating pivot SQL for large table

        Handy tools if you use Redshift and would like to pivot a table.

        Args:
            cursor: a PostgreSQL cursor
            tableName: the source tablename (add schema to tablename if applied, i.e schema.table)
            rowBy: the column name of the unique key which identifies multiple records from the same object
            columnBy: the column name of the variable which differentiates multiple records from the same object
            valueNames: a list of column names of values
            groupFuncName: The function is used to aggregate value, and the default is SUM
            isTemp: if True, the target table will be a temporarily table
            prefix: if isTemp is False, prefix will be the schema name where the target table will be store
            columnFilter: a function that return True or False to filter out unwanted values in columnBy

        Returns:
            None


        Example:
            source table: sandbox.example_table
            
                        uid     subject     midterm     final
                    -----------------------------------------------
                        1       English     98          98
                        1       Math        100         95
                        2       Math        20          60
                        2       Other       22          25

            the target table of pivotSQLGenerate(cursor, tableName = "sandbox.example_table", rowBy = "uid", columnBy = "subject",
                                                 valueNames = ["score", "final"], isTemp = False,
                                                 prefix = "sandbox", columnFilter = lambda x: x != "Other") will be:

            target table: sandbox.example_table_pivot_uid__subject

                        uid     English__midterm    English__final  Math__midterm   Math__final
                    ------------------------------------------------------------------------------
                        1       98                      98                 100          95
                        2       null                    null                20          60

    """
 
    # define the result table name
    resultTableName = tableName + "_pivot_" + rowBy + "__" + columnBy
    if not isTemp:
        assert isinstance(prefix, str)
        resultTableName = prefix + "." + resultTableName
 
    # define TEMP indicator
    tempIndicator = "TEMP" if isTemp else ""
 
    # create column by value
    sql = """SELECT DISTINCT %s FROM %s """ % (columnBy, tableName)
    cursor.execute(sql)
    uniqueColumnValues = cursor.fetchall()
 
    uniqueColumnValues = [x[0] if x[0] is not None else "Null" for x in uniqueColumnValues]
 
    # apply column filter if available
    if columnFilter:
        assert callable(columnFilter)
        filter = map(columnFilter, uniqueColumnValues)
        uniqueColumnValues = [v for (v, b) in zip(uniqueColumnValues, filter) if b]
 
    # check if multiple valueName passed
    if isinstance(valueNames, str):
        valueNames = (valueNames,)
 
    # generating SQL
    columnGeneratorSql = ""
    columnGeneratorArg = []
    for valueName in valueNames:
        for columnValue in uniqueColumnValues:
            if columnValue:
                colmunName = valueName + "__" + re.sub("\W", "_", columnValue)
                colmunName = colmunName.replace(" ", "_")
                sql = ", " + "%s(CASE WHEN %s = %%s THEN %s ELSE null END) AS %s" % (groupFuncName, columnBy, valueName, colmunName)
                columnGeneratorArg.append(columnValue)
                columnGeneratorSql += sql
 
    sql = """CREATE %s TABLE %s
             AS
             SELECT %s%s
             FROM %s
             GROUP BY %s;""" % (tempIndicator, resultTableName, rowBy, columnGeneratorSql, tableName, rowBy)
 
    # execute SQL
    cursor.execute("DROP TABLE IF EXISTS %s CASCADE;" % resultTableName)
    cursor.execute(sql, columnGeneratorArg)
 
    return resultTableName
 
if __name__ == "__main__":
    pass