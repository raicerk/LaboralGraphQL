//Inyección de dependencias
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const registerRoutes = require('./routes');

//Configuración
const config = require('./config');

//Inicialización de la aplicación
const app = express();

//Configuración de la API
require('events').EventEmitter.defaultMaxListeners = Infinity;

//Configuración de nuestra API
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({
  extended: true
}));

//Configuración de seguridad de la API
app.use(cors());

//Configuración del puerto de la API
app.set('port', config.puerto);

//Configuración de la cabecera
app.use(function (req, res, next) {
  res.setHeader('Access-Control-Allow-Origin', `http://${config.dominio}`);
  res.setHeader('Access-Control-Allow-Methods', 'GET');
  res.setHeader('Content-Type', 'application/json');
  next();
});

/**
 * Ruta de bienvenida
 * @param Ruta,
 * @param function
 * @returns { json }
 */
app.get('/', function(req, res) {
  res.send({
    'message': 'Bienvenido a la API de datos estadísticos laborales tecnológicos'
  });
});

/**
 * Registro de ruta inicial
 * @param app,
 * @returns { void }
 */
registerRoutes(app);

/**
 * Inicialización del servicio
 * @param puerto,
 * @param function
 * @returns { void }
 */
app.listen(config.puerto, function() {
  console.log(`Node server ejecutandose en http://${config.dominio}:${config.puerto}`);
});