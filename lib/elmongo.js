var request = require('request'),
    mongoose = require('mongoose'),
    util = require('util'),
    url = require('url'),
    helpers = require('./helpers'),
    sync = require('./sync'),
    EE = new require('events').EventEmitter(),
    _ = require('underscore');

// turn off request pooling
request.defaults({ agent:false })

// cache elasticsearch url options for elmongo.search() to use
var elasticUrlOptions = null

/**
 * Attach mongoose plugin for elasticsearch indexing
 *
 * @param  {Object} schema      mongoose schema
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
module.exports = elmongo = function (schema, options) {
    // add events pub/sub for schema
    _.mixin(schema, EE);

    // attach methods to schema
    schema.methods.index = index
    schema.methods.unindex = unindex
    schema.methods.customIndex = customIndex
    schema.methods.customRemoveIndex = customRemoveIndex

    schema.statics.sync = function (cb) {
        options = helpers.mergeModelOptions(options, this)

        return sync.call(this, schema, options, cb)
    }

    schema.statics.search = function (searchOpts, cb) {
        options = helpers.mergeModelOptions(options, this)

        var searchUri = helpers.makeTypeUri(options) + '/_search?search_type=dfs_query_then_fetch&preference=_primary_first'

        return helpers.doSearchAndNormalizeResults(searchUri, searchOpts, cb)
    }

    // attach mongoose middleware hooks
    schema.post('save', function () {
        options = helpers.mergeModelOptions(options, this)
        this.index(options)
    })
    schema.post('remove', function () {
        options = helpers.mergeModelOptions(options, this)
        this.unindex(options)
    })

    schema.on('middlewareUpdate', function (data, opts, callback) {
        if (opts)
            options = opts;

        this.methods.customIndex(options, data, callback);
    })

    schema.on('middlewareRemove', function (opts, id, callback) {
        if (opts)
            options = opts;

        this.methods.customRemoveIndex(opts, id, callback)
    })
}

/**
 * Search across multiple collections. Same usage as model search, but with an extra key on `searchOpts` - `collections`
 * @param  {Object}   searchOpts
 * @param  {Function} cb
 */
elmongo.search = function (searchOpts, cb) {
    // merge elasticsearch url config options
    elasticUrlOptions = helpers.mergeOptions(elasticUrlOptions)

    // determine collections to search on
    var collections = searchOpts.collections;

    if (elasticUrlOptions.prefix) {
        // prefix was specified - namespace the index names to use the prefix for each collection's index

        if (searchOpts.collections && searchOpts.collections.length) {
            // collections were specified - prepend the prefix on each collection name
            collections = collections.map(function (collection) {
                return elasticUrlOptions.prefix + '-' + collection
            })
        } else {
            // no collections specified, but prefix specified - use wildcard index with prefix
            collections = [ elasticUrlOptions.prefix + '*' ]
        }
    } else {
        // no prefix used

        // if collections specified, just use their names without the prefix

        if (!collections) {
            // no collections were specified so use _all (searches all collections), without prefix
            searchOpts.collections = [ '_all' ]
        }
    }

    var searchUri = helpers.makeDomainUri(elasticUrlOptions) + '/' + collections.join(',') + '/_search?search_type=dfs_query_then_fetch&preference=_primary_first'

    return helpers.doSearchAndNormalizeResults(searchUri, searchOpts, cb)
}

/**
 * Configure the Elasticsearch url options for `elmongo.search()`.
 *
 * @param  {Object} options - keys: host, port, prefix (optional)
 */
elmongo.search.config = function (options) {
    // only overwrite `options` values that are being specified in this call to `config`
    if (elasticUrlOptions) {
        Object
        .keys(elasticUrlOptions)
        .forEach(function (key) {
            elasticUrlOptions[key] = options[key] || elasticUrlOptions[key]
        })
    }
    // normalize the `options` object
    elasticUrlOptions = helpers.mergeOptions(options)
}

/**
 * Index a document in elasticsearch (create if not existing)
 *
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
function index (options) {
    var self = this
    // strip mongoose-added functions, depopulate any populated fields, and serialize the doc
    var esearchDoc = helpers.serializeModel(this, options.serializeOptions);

    var indexUri = helpers.makeDocumentUri(options, self)

    var reqOpts = {
        method: 'PUT',
        url: indexUri,
        body: JSON.stringify(esearchDoc)
    }

    // console.log('index:', indexUri)

    helpers.backOffRequest(reqOpts, function (err, res, body) {
        if (err) {
            var error = new Error('Elasticsearch document indexing error: '+util.inspect(err, true, 10, true))
            error.details = err

            self.emit('error', error)
            return
        }

        self.emit('elmongo-indexed', body)
    })
}

/**
 * Remove a document from elasticsearch
 *
 * @param  {Object} options     elasticsearch options object. Keys: host, port, index, type
 */
function unindex (options) {
    var self = this

    var unindexUri = helpers.makeDocumentUri(options, self)

    console.log('unindex:', unindexUri)

    var reqOpts = {
        method: 'DELETE',
        url: unindexUri
    }

    helpers.backOffRequest(reqOpts, function (err, res, body) {
        if (err) {
            var error = new Error('Elasticsearch document index deletion error: '+util.inspect(err, true, 10, true))
            error.details = err

            self.emit('error', error)
            return
        }

        self.emit('elmongo-unindexed', body)
    })
}

/**
 * @author: huytran
 *
 * Custom remove index record by id
 */
function customRemoveIndex (options, id, callback) {
    var self = this,
        unindexUri = helpers.makeDocumentUri(options, {_id: id});

    // console.log(options)

    console.log('unindexUri--', unindexUri);

    var reqOpts = {
        method: 'DELETE',
        url: unindexUri
    }

    helpers.backOffRequest(reqOpts, function (err, res, body) {
        if (err) {
            var error = new Error('Elasticsearch document index deletion error: '+util.inspect(err, true, 10, true))
            error.details = err

            return callback(error);
        }

        callback(null, body);
    })
}

/**
 * @author: huytran
 * Custom re-index company to elastic search after update
 */

function customIndex (options, data, callback) {

    var self = this,
        indexUri = helpers.makeDocumentUri(options, data)

    // data.__proto__.toObject = function () { return this; };

    var esearchDoc = helpers.serializeModel(data, options.serializeOptions);

    // console.log(esearchDoc);

    var reqOpts = {
        method: 'PUT',
        url: indexUri,
        body: JSON.stringify(esearchDoc)
    }

    // console.log('index:', indexUri)

    helpers.backOffRequest(reqOpts, function (err, res, body) {
        if (err) {
            var error = new Error('Elasticsearch document indexing error: '+util.inspect(err, true, 10, true))
            error.details = err

            // self.emit('error', error)
            return callback && callback(err);
        }

        // self.emit('elmongo-indexed', body)
        callback && callback();
    })
}
