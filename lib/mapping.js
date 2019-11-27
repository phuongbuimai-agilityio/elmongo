var util = require('util'),
	mongoose = require('mongoose'),
	Schema = mongoose.Schema,
	ObjectId = require('mongodb').ObjectID

/**
 * Deep-traverse a Mongoose schema and generate an elasticsearch mapping object
 * @param  {Object} schema - mongoose schema
 * @return {Object}
 */
exports.generateMapping = function (schema) {
	// console.log('generateMapping schema.tree', schema.tree)
	// console.log('generateMapping schema.paths', util.inspect(schema.paths, true, 3, true))

    var mapping = {
		properties: {

		}
	}
	
	Object
	.keys(schema.paths)
	.forEach(function (path) {
		// the mongoose information associated to the path
		var pathInfo = schema.paths[path];

    var pathArray = path.split('.')

		var currentLocation = mapping.properties

		// build out the mapping object by traversing the path defined by `pathArray` and building it in `mapping`
		pathArray.forEach(function (pathEntry, i) {
			if (!currentLocation[pathEntry])
				currentLocation[pathEntry] = {}

			if (i === pathArray.length - 1) {
				// we're at the lowest level of the mapping object for this `path`. Set the elasticsearch mapping info for it.

				// determine the type to set on the field in `mapping`
				if (pathInfo.instance) {
          var instanceName = pathInfo.instance.toLowerCase()

				 	if (instanceName === 'objectid') {
						currentLocation[pathEntry].type = 'string'
					} else if (instanceName === 'number') {
						currentLocation[pathEntry].type = 'double'
          } else if (instanceName === 'array') {
            if (pathInfo.options.indexType === 'completion')
            currentLocation[pathEntry].type = pathInfo.options.indexType
          } else {
            currentLocation[pathEntry].type = pathInfo.instance.toLowerCase()
					}
				} else if (pathInfo.caster && pathInfo.caster.instance) {
					if (pathInfo.caster.instance.toLowerCase() === 'objectid') {
						currentLocation[pathEntry].type = 'string'
					} else {
            var type = pathInfo.caster.instance.toLowerCase()

						if (type === 'number') {
							currentLocation[pathEntry].type = 'double'
						} else {
							currentLocation[pathEntry].type = type
						}
					}
				} else if (pathInfo.options) {

					// ThuongDinh: allow indexType
					if (pathInfo.options.indexType) {
						currentLocation[pathEntry].type = pathInfo.options.indexType;
					} else {

						// console.log('pathInfo.options', pathInfo.options)
						var typeClass = pathInfo.options.type

						if (Array.isArray(typeClass)) {
							if (!typeClass[0]) {
								currentLocation[pathEntry].type = 'object'
							} else {
								// this low-level property in the schema is an array. Set type of the array entries
								var arrEntryTypeClass = typeClass[0].type

								currentLocation[pathEntry].type = exports.getElasticsearchTypeFromMongooseType(arrEntryTypeClass)
							}
						} else {
							// `options` exists on pathInfo and it's not an array of types
							currentLocation[pathEntry].type = exports.getElasticsearchTypeFromMongooseType(typeClass)
						}
					}
				}

				if (!currentLocation[pathEntry].type) {
					// default to object type
					currentLocation[pathEntry].type = 'object'
        }

				// set autocomplete analyzers if user specified it in the model
				if (
					(pathInfo.options && pathInfo.options.autocomplete) ||
					(pathInfo.caster && pathInfo.caster.options && pathInfo.caster.options.autocomplete)
				) {
					currentLocation[pathEntry].index_analyzer = 'autocomplete_index'
					currentLocation[pathEntry].search_analyzer ='autocomplete_search'
				}

				// set analizer whitespace
				if (
					(pathInfo.options && pathInfo.options.whitespace) ||
					(pathInfo.caster && pathInfo.caster.options && pathInfo.caster.options.whitespace)
				) {

					currentLocation[pathEntry].index_analyzer = 'whitespace'
					currentLocation[pathEntry].search_analyzer ='whitespace'
				}
				// set index type
				if (pathInfo.options && pathInfo.options.es_index) {
          if (pathInfo.options.es_index == 'multi_field' && pathInfo.options.indexType === 'completion') {
            currentLocation[pathEntry].type = 'completion';
            currentLocation[pathEntry].fields = pathInfo.options.fields;
          } else if (pathInfo.options.es_index == 'multi_field' && pathInfo.options.fields && pathInfo.options.indexType !== 'geo_point') {
						// multi field index
						currentLocation[pathEntry].type = 'text';
            currentLocation[pathEntry].fields = pathInfo.options.fields;
					} else if (pathInfo.options.es_index == 'not_analyzed') {
						// not analyzed index
            currentLocation[pathEntry].es_index = 'keyword';
            currentLocation[pathEntry].type = 'keyword';
					}
        }

        if (pathInfo.options && pathInfo.options.indexType === 'nested') {
          currentLocation[pathEntry].type = 'nested';
        }

        if (pathInfo.options && pathInfo.options.indexType === 'geo_point') {
          currentLocation[pathEntry].type = 'geo_point';
        }

        if (pathInfo.options && pathInfo.options.es_index === 'keyword') {
          currentLocation[pathEntry].type = 'keyword';
        }

        if (pathInfo.options && pathInfo.options.es_index === 'string') {
          currentLocation[pathEntry].type = 'text';
        }

        // set fielddata if user specified it in the model
        // if (
        //   (pathInfo.options && pathInfo.options.fielddata) ||
        //   (pathInfo.caster && pathInfo.caster.options && pathInfo.caster.options.fielddata)
        // ) {
        //   currentLocation[pathEntry].fielddata = true;
        // }

        // set caseinsensitive if user specified it in the model
        if (
          (pathInfo.options && pathInfo.options.caseinsensitive) ||
          (pathInfo.caster && pathInfo.caster.options && pathInfo.caster.options.caseinsensitive)
        ) {
          currentLocation[pathEntry].index_analyzer = 'analyzer_case_insensitive'
					currentLocation[pathEntry].search_analyzer ='analyzer_case_insensitive'
        }

			} else {
				// mark this location in the mapping as an object (only set if it hasn't been set by a previous path already)
				if (!currentLocation[pathEntry].properties) {
					currentLocation[pathEntry].properties = {}
				}

				// keep going deeper into the mapping object - we haven't reached the end of the `path`.
				currentLocation = currentLocation[pathEntry].properties
      }
		})
	})

  // console.log('\ngenerateMapping - mapping:', util.inspect(mapping, true,10, true))

  function eachRecursive(obj) {
    for (var k in obj) {
      if (!obj.fields && obj.es_index !== 'keyword' && obj.es_index !== 'multi_field' && obj[k] === 'string' || obj[k] === 'text') {
        obj['fielddata'] = true;
      }

      if (typeof obj[k] === 'object' && obj[k] !== null) {
        eachRecursive(obj[k]);
      } else {
        if (obj[k] === 'string' || obj[k] === 'String' || obj[k] === 'multi_field') {
          obj[k] = 'text';
        } else if (obj[k] === 'array' || obj[k] === 'mixed') {
          obj[k] = 'object';
        }

        if (k === 'index_analyzer') {
          obj['analyzer'] = obj[k];

          delete obj[k];
        }

        if (k === 'es_index') {
          if (typeof obj[k] === 'string') {
            obj['type'] = obj[k];
          }

          delete obj[k];
        }

        if (k === 'autocomplete') {
          delete obj[k];
        }

      }
    }
  }

  eachRecursive(mapping);

  delete mapping.properties._id;

  // var util = require('util')
  // console.log(util.inspect(mapping, {showHidden: false, depth: null}))

	return mapping
}

exports.getElasticsearchTypeFromMongooseType = function (typeClass) {
	if (typeClass === String) {
		return 'text'
	}
	if (typeClass === Number) {
		return 'double'
	}
	if (typeClass === Date) {
		return 'date'
	}
	if (typeClass === Boolean) {
		return 'boolean'
	}
	if (typeClass === Array) {
		return 'object'
  }
}
