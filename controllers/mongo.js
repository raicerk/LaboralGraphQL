const MongoClient = require('mongodb').MongoClient;
const config = require('../config')

class Mongo {

  static ConnectDB() {
    try {
      let url = `mongodb+srv://${config.db.DB_USER}:${config.db.DB_PASSWORD}@${config.db.DB_HOST}/${config.db.DB_NAME}`;
      return new Promise((resolve, reject) => {
        var settings = {
          reconnectTries: Number.MAX_VALUE,
          reconnectInterval: 100,
          autoReconnect: true,
          useNewUrlParser: true,
          useUnifiedTopology: true,
          poolSize: 2

        };
        let client = new MongoClient(url, settings);
        client.connect().then(() => {
          resolve(client.db(config.db.DB_NAME))
        }).catch(err => {
          reject(err)
        })
      })
    } catch (error) {
      nl.register(`Error en la conexion de la base de datos de mongo historico, el detalle es: ${error}`).error();
    }
  }
}


module.exports = Mongo