const connections = require('./connections');
const mssql = require('mssql');

function executeSql(config, queryConfig) {

    const sqlConnection = connections.get(config);
    const request = new mssql.Request(sqlConnection);

    // params for query
    if (queryConfig.params) {
        Object.keys(queryConfig.params).forEach(function (key) {
            const param = queryConfig.params[key];
            if (typeof param !== 'object') {
                request.input(key, param);
            } else {
                if (param.type) {
                    request.input(key, param.type, param.value);
                } else {
                    request.input(key, param.value);
                }
            }
        });
    }

    // query to be executed
    const query = queryConfig.query;
    return request.query(query);
    
}

function generateColumnsQuery(tableName) {
    return `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS  WHERE TABLE_NAME = '${tableName}'`;
}

function generateKeysQuery(tableName) {
    return (
        'SELECT KU.table_name as TABLENAME, column_name as PRIMARYKEYCOLUMN ' +
        'FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC ' +
        'INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU ' +
        `ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND ` +
        'TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND '+
        `KU.table_name='${tableName}' ` +
        'ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;'
    );
}

function generateUpsertQuery(tableName, tableSchema, columns, keys) {
    let updateQuery = `UPDATE ${tableSchema}.${tableName} SET `;
    let updateKeys = `WHERE `;
    let insertQueryAttributes = `INSERT INTO ${tableSchema}.${tableName} `;
    let insertQueryValues = 'VALUES ';
    for (let i = 0; i < columns.length; i++) {
        if (i === 0) {
            insertQueryAttributes += `(${columns[i]}, `;
            insertQueryValues += `(@${columns[i]}, `;
            updateQuery += `${columns[i]} = @${columns[i]}, `;
        } else if (i === columns.length - 1) {
            insertQueryAttributes += `${columns[i]}) `;
            insertQueryValues += `@${columns[i]}) `;
            updateQuery += `${columns[i]} = @${columns[i]} `;
            for (let j = 0; j < keys.length; j++) {
                if (j === keys.length - 1) {
                    updateKeys += `${keys[j]} = @${keys[j]};`;
                } else {
                    updateKeys += `${keys[j]} = @${keys[j]} AND `;
                }
            }
        } else {
            insertQueryAttributes += `${columns[i]}, `;
            insertQueryValues += `@${columns[i]}, `;
            updateQuery += `${columns[i]} = @${columns[i]}, `;
        }
    }
    return (
        'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;' +
        'BEGIN TRANSACTION;' +
        updateQuery + updateKeys +
        'IF @@ROWCOUNT = 0 ' +
        'BEGIN ' +
        insertQueryAttributes + 
        insertQueryValues +
        'END ' +
        'COMMIT TRANSACTION;'
    );
}

module.exports = {
    generateUpsertQuery: generateUpsertQuery,
    generateKeysQuery: generateKeysQuery,
    generateColumnsQuery: generateColumnsQuery,    
    executeSql: executeSql  
}