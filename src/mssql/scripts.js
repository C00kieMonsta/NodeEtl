const helpers = require('./helpers');
const connections = require('./connections');

module.exports = function(config, queries) {
    connections.add(config).then(() => {
        return Promise.all(queries.map((q) => helpers.executeSql(config, q)))
        .then(() => {
            connections.get(config).close();
        });
    });
}