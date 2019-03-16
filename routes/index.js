const graphql = require('./graphql');
const cache = require('../util/cache-middleware');
const config = require('../config');

/**
 * Registro de rutas
 *
 * @param {Express.Application} app Aplicación de Express
 * @returns {void}
 */
const register = function(app) {
  app.use('/graphql', cache(config.cache), graphql);
};

module.exports = register;
