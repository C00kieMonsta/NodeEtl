const mssql = require('mssql');
const Promise = require('bluebird');

const state = {
    pools: {},
    connections: {},
    configurations: {},
};

const connections = {
    add: addConnection,
    get: getConnection
};

function addConnection(config) {
    const name = getConnectionName(config);

    if (config.host) {
        config.server = config.host;
    }

    return createConnection(name, config);
}

function getConnection(config) {
    let name = getConnectionName(config);
    const connection = state.connections[name];
    if (!connection) {
        throw new Error('No connections');
    }
    return connection;
}

function createConnection(name, config) {
    const pool = new mssql.Connection(config);

    pool.on('close', function() {
        pool.removeAllListeners();
        delete state.connections[name];
        delete state.pools[name];
    });

    pool.on('error', function (error) {
        pool.removeAllListeners();
        delete state.connections[name];
        delete state.pools[name];
    });

    state.pools[name] = pool;

    return new Promise(function (resolve, reject) {
        pool.connect().then(function (conn) {
            state.connections[name] = conn;
            resolve(conn);
        }, function (error) {
            pool.removeAllListeners();
            delete state.connections[name];
            delete state.pools[name];
            reject(error);
        });
    });
}

function getConnectionName(config) {
    if (typeof config === 'string') {
        return config;
    } else if (config.name) {
        return config.name;
    } else {
        return 'default';
    }
}

module.exports = connections;