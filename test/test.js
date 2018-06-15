const config = require('../config');
const etl = require('../src/mssql/index');

const createSwapPointTable = `CREATE TABLE SWAP_POINTS (internalRef INT NOT NULL, entity VARCHAR(256) NOT NULL, tradeDate DATETIME, maturity DATETIME, purchasedCurrency VARCHAR(256) NOT NULL, purchasedAmount FLOAT NOT NULL, soldCurrency VARCHAR(256) NOT NULL, soldAmount FLOAT NOT NULL, swapPoints FLOAT NOT NULL, valuation FLOAT NOT NULL, batchDate DATETIME NOT NULL CONSTRAINT CreateTS_DF DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (internalRef))`;
const dropSwapPointTable = `DROP TABLE SWAP_POINTS`;
const tableName = 'SWAP_POINTS';
const tableSchema = 'dbo';
const DATA_PATH = `${__dirname}/../data/*.csv`;

/**
 * Create table(s)
 */
// etl.scripts(config, [{ query: createSwapPointTable }]);

/**
 * Insert or update data inside table
 */
etl.upsert(config, tableName, tableSchema, DATA_PATH);


/**
 * Drop table(s)
 */
// etl.scripts(config, [{ query: dropSwapPointTable }]);