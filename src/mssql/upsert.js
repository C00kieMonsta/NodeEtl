const combine = require('combined-stream');
const glob = require('glob');
const fs = require('fs');
const csvReader = require('csv-parser');

const connections = require('./connections');
const helpers = require('./helpers');

module.exports = function(config, tableName, tableSchema, dataPath) {
    connections
        .add(config)
        .then(() => {
            let filesToProcess = [];
            return new Promise((resolve, reject) => {
                glob(dataPath, (err, files) => {
                    if (err) {
                        return reject(err)
                    }
                    resolve(files);
                })
            }).then((files) => {
                if (files.length === 0) {
                    console.log('No data')
                    return connections.get(config).close();
                }
                filesToProcess = files;
            }).then(() => {

                const columnsQuery = helpers.generateColumnsQuery(tableName);
                const keysQuery = helpers.generateKeysQuery(tableName);

                return new Promise((resolve) => {
                    helpers.executeSql(config, {query: columnsQuery}).then((cols) => {
                        helpers.executeSql(config, {query: keysQuery}).then((ks) => {
                            resolve({
                                columns: cols.map(d => d.COLUMN_NAME).filter(i => i !== 'batchDate'),
                                keys: ks.map(d => d.PRIMARYKEYCOLUMN)
                            });
                        })
                    })
                })
            }).then((result) => {

                // retrieve column names of table
                if (!result) {
                    throw 'TABLE_NOT_FOUND';
                }

                const upsertQuery = helpers.generateUpsertQuery(tableName, tableSchema, result.columns, result.keys);

                // put all files in one read stream
                const stream = combine.create();
                filesToProcess.sort().map(e => {
                    stream.append(fs.createReadStream(e))
                });

                // convert csv files
                stream.pipe(csvReader({headers: result.columns}))
                    .on('data', function (d) {
                        helpers.executeSql(config, {query: upsertQuery, params: d});
                    })
                    .on('end', function () {
                        connections.get(config).close();
                    })

            })
        })
}