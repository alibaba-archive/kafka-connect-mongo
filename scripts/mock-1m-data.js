// mongo 192.168.0.21:27017/kafka -u root -p root --authenticationDatabase admin mock-1m-data.js
db = db.getSiblingDB('kafka')

for (var i = 0; i < 100; i++) {
    var bulk = []
    for (var n = 0; n < 10000; n ++) {
        bulk.push({
            insertOne: {
                document: {
                    key: Math.random(),
                    ts: Timestamp(Math.floor(Date.now() / 1000), 0)
                }
            }
        })
    }
    db.t.bulkWrite(bulk)
    print('Write ' + (i + 1) * 10000 + ' documents')
}