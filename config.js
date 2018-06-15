const config = {
    name: "etl-connection",
    user: "sa",
    password: "reallyStrongPwd123",
    server: "localhost",
    database: "etl_server",
    port: 1433,
    options: {
        "encrypt": false
    }
}

module.exports = config;