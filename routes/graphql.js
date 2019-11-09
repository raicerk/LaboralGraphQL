const graphqlHTTP = require('express-graphql');
const db = require("../controllers/db");
const { buildSchema } = require('graphql');
const router = require("express").Router();

/**
 * Constructor de equema de datos de grahql
 */
const schema = buildSchema(`

  """Datos estadisticos sobre ofertas laborales y conocimientos requeridos para cada pais en el mundo de la tecnologia"""
  type Query {
    """Datos estadisticos con filtros"""
    Laboral(where: FieldBy, order: OrderBy): [Laboral],
    """Datos estadisticos completos"""
    Laborales: [Laboral]
    """Datos agrupados por fecha"""
    LaboralAgrupadoPorMes(where: FieldBy): [Agrupacion]
    """Datos agrupados por fecha"""
    LaboralAcumulado(where: FieldBy): [Cantidad]
    """Datos de salarios con filtros"""
    LaboralSalarios(where: FieldBy): [Salario]
    """Datos de skill que son solicitados al seleccionar un skill"""
    LaboralConOtrosSkill(country: Country, skill: Skill): [Cantidad]
  }

  input OrderBy {
    by: String
    orientation: String
  }

  input FieldBy{
    field: String,
    value: String,
    in: String
  }

  input Country{
    value: String
  }

  input Skill{
    value: String
  }

  """Datos de ofertas laborales por año mes y cantidad"""
  type Datos{
    """Año mes del skill"""
    fecha: String
    """Cantidad de ofertas laborales donde el skill es solicitado para la fecha que se indica"""
    cantidad: Int
  }

  """Campos disponibles de las ofertas laborales y por los cuales se puede filtrar"""
  type Laboral {
    """Fecha de la oferta"""
    fecha: String
    "Pais de publicación"
    pais: String
    "Link de acceso"
    link: String
    "Clasificacion de la oferta"
    clasificacion: String
    "Sueldo ofrecido en la oferta"
    sueldo: String
    "Sueldo Minimo ofrecido en la oferta"
    sueldominimo: Int
    "Sueldo Maximo ofrecido en la oferta"
    sueldomaximo: Int
    "Tipo de moneda del sueldo ofrecido en la oferta"
    sueldomoneda: String
    "Modalidad de tiempo de empleo ofrecido en la oferta"
    sueldotipotiempo: String
    "Skill requeridos en la oferta"
    skill: [String]
  }

  """Campos disponibles para mostrar datos agrupados"""
  type Agrupacion{
    """Nombre del skill"""
    skill: String
    """Datos agrupados del skill"""
    datos: [Datos]
  }

  """Campos disponibles para mostrar datos agrupados"""
  type Cantidad{
    """Nombre del skill"""
    skill: String
    """Cantidad de skill agrupados"""
    cantidad: Int
  }

  type Salario{
    """Nombre del skill"""
    skill: String
    """Promedio de salario minimo ofrecido en la oferta"""
    salariominimo: Int 
    """Promedio de salario maximo ofrecido en la oferta"""
    salariomaximo: Int 
    """Media de salario ofrecido en la oferta"""
    media: Int
    """Cantidad de ofertas que incluyen el skill indicadado"""
    cantidad: Int
  }
`);

/**
 * Definición de las funciones que seran llamadas por el modelo de graphql
 */
const root = {
  Laboral: async ({ where, order }) => {
    const snapshot = await connMongo.collection("laboral").find({ [where.field]: where.value, fecha: { $gt: "2018-12-01T00:00:00" } }).sort({ [order.by]: order.orientation == "desc" ? 1 : -1 }).toArray()
    return snapshot;
  },
  Laborales: async () => {
    const snapshot = await connMongo.collection("laboral").find({fecha: { $gt: "2018-12-01T00:00:00" }}).toArray()
    return snapshot;
  },
  LaboralAgrupadoPorMes: async ({ where }) => {
    const snapshot = await connMongo.collection("laboral").find({ [where.field]: where.value, fecha: { $gt: "2018-12-01T00:00:00" } }).toArray()
    const data = snapshot;

    const rawSkills = [];
    data.forEach((entry) => rawSkills.push(...entry.skill));
    const skills = [...new Set(rawSkills)];
    var iib = [];


    skills.map((value) => {

      const datoSkill = {};

      var dattta = [];

      data.forEach((entry) => {
        let dato = 0;
        // Fecha de evaluacion del ciclo
        const fecha = `${entry.fecha.split('-')[0]}-${entry.fecha.split('-')[1]}`;
        // Si la entrada en el origen de datos contiene la skill
        if (entry.skill.findIndex(s => s === value) !== -1) {

          dato = datoSkill[fecha];
          // Si el mes existe en datos, agrega 1, si no lo crea con valor 1
          if (dato) {
            datoSkill[fecha] = dato + 1;
          } else {
            datoSkill[fecha] = 1;
          }
        }
      });

      for (var i in datoSkill) {
        dattta.push({
          fecha: i,
          cantidad: datoSkill[i]
        });
      }

      dattta.sort((a, b) => a.fecha < b.fecha ? 1 : -1);

      iib.push({
        skill: value,
        datos: dattta
      })
    })

    return iib.sort((x, y) => x.skill > y.skill ? 1 : -1);

  },
  LaboralAcumulado: async ({ where }) => {
    //{$in: where.in.split(",")}
    const snapshot = await connMongo.collection("laboral").find({ [where.field]: where.value , fecha: { $gt: "2018-12-01T00:00:00" }}).toArray()
    const data = snapshot;

    const rawSkills = [];
    data.forEach((entry) => rawSkills.push(...entry.skill));
    const skills = [...new Set(rawSkills)];
    var iib = [];

    skills.map((value) => {

      var datta = 0;

      data.forEach((entry) => {
        if (entry.skill.findIndex(s => s === value) !== -1) {
          datta++
        }
      });

      iib.push({
        skill: value,
        cantidad: datta
      })
    })

    return iib.sort((x, y) => x.skill > y.skill ? 1 : -1);

  },
  LaboralSalarios: async ({ where }) => {
    const snapshot = await connMongo.collection("laboral").aggregate([
      {
        '$unwind': {
          'path': '$skill'
        }
      }, {
        '$match': {
          [where.field]: where.value,
          fecha: { $gt: "2018-12-01T00:00:00" },
          'sueldominimo': {
            '$ne': null
          }
        }
      }, {
        '$group': {
          '_id': '$skill',
          'averageMin': {
            '$avg': '$sueldominimo'
          },
          'averageMax': {
            '$avg': '$sueldomaximo'
          },
          'count': {
            '$sum': 1
          }
        }
      }, {
        '$addFields': {
          'suma': {
            '$sum': [
              '$averageMax', '$averageMin'
            ]
          }
        }
      }, {
        '$addFields': {
          'media': {
            '$divide': [
              '$suma', 2
            ]
          }
        }
      }, {
        '$project': {
          'suma': 0
        }
      }
    ]).toArray()

    return snapshot.map(iter => {
      return {
        skill: iter._id,
        salariominimo: Math.round(iter.averageMin),
        salariomaximo: Math.round(iter.averageMax),
        media: Math.round(iter.media),
        cantidad: iter.count
      }
    });
  },
  LaboralConOtrosSkill: async ({ country, skill }) => {
    const snapshot = await connMongo.collection("laboral").aggregate([
      {
        '$match': {
          skill: skill.value,
          pais: country.value,
          fecha: { $gt: "2018-12-01T00:00:00" }
        }
      }, {
        '$unwind': {
          'path': '$skill'
        }
      }, {
        '$group': {
          '_id': '$skill',
          'count': {
            '$sum': 1
          }
        }
      }, {
        '$sort': {
          'count': -1
        }
      }
    ]).toArray()
    return snapshot.map(iter => {
      return {
        skill: iter._id,
        cantidad: iter.count
      }
    })
  }
};

/**
 * Llamado a la función de graphql
 */
router.use('/', graphqlHTTP({
  schema,
  rootValue: root,
  graphiql: true
}));

module.exports = router;