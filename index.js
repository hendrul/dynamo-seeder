'use strict';
var vm = require('vm');
var path = require('path');
var AWS = require('aws-sdk');

module.exports = (function () {
	var DEFAULT_OPTIONS = {
		dropTables: false
	};

	var $this = {
		options: {},
		sandbox: {},
		seed: function (dataFile) {
			var self = this;
			var data = require(dataFile);

			try {
				// Iterate over all the dependencies
				Object.keys(data['_dependencies'] || {}).forEach(function (key) {
					if (self.sandbox[key] !== undefined) {
						// Do nothing if the dependency is already defined globally
						return;
					}

					// Load the dependency
					self.sandbox[key] = module.parent.require(data['_dependencies'][key]);
				});

				delete data['_dependencies'];

				var promises = Object.keys(data).map(tableName => {

					// Schema path is relative to the referer (a data file)
					var schemaPath = path.resolve(path.dirname(dataFile), data[tableName]['_schema']);
					var schema = require(schemaPath);

					delete data['_schema'];

					return ()=> {
						var promise = Promise.resolve();
						if (self.options.dropTables === true) {
							promise = promise.then(()=> $this.Dynamo.deleteTable({TableName: tableName}));
						}

						// Always create the table
						promise = promise.then(()=> $this.Dynamo.createTable(schema));

						// Insert records promises
						return promise.then(
							()=> Promise.all(
								(data[tableName].data||[]).map(record => {
									var unwinded = $this.unwind(record);
									return $this.dynamo.insert({
										TableName: tableName,
										Item: unwinded
									});
								})
							)
						);
					}
				});

				var serie = Promise.resolve();
				promises.forEach(runPromise => serie = serie.then(()=> runPromise()));

				return serie;
			} catch (err) {
				// Reject the method if something went wrong
				return Promise.reject(err);
			}
		},
		/**
		 * This method unwinds an object and iterates over every property in the object.
		 * It will then parse the value of the property in order to search for references
		 * and make a reference to the correct object.
		 *
		 * @param  {Object} obj The object to parse.
		 * @return {Object}	 The object with the correct references.
		 */
		unwind: function (obj) {
			for(var key in obj) {
				obj[key] = $this.parseValue(obj, obj[key]);
			}
			return obj;
		},
		/**
		 * This method parses every value. If the value is an object it will unwind
		 * that object as well. If the value is a reference (value starting with ->),
		 * then it will find the reference to that object.
		 *
		 * @param  {Object} parent  The object for which the value should be parsed.
		 * @param  {*}	  value   The value that should be parsed.
		 * @return {*}			  The parsed value.
		 */
		parseValue: function (parent, value) {
			if (Array.isArray(value)) {
				// Iterate over the array
				return value.map(function (val) {
					return $this.parseValue(parent, val);
				});
			} else if (typeof value === 'object') {
				// Unwind the object
				return $this.unwind(value);
			} else if (typeof value === 'string' && value.indexOf('=') === 0) {
				// Evaluate the expression
				try {
					// Assign the object to the $this property
					var base = {$this: parent};

					// Create a new combined context
					var ctx = vm.createContext(Object.assign(base, $this.sandbox));

					// Run in the new context
					return vm.runInContext(value.substr(1).replace(/this\./g, '$this.'), ctx);
				} catch (e) {
					return value;
				}
			} else if (typeof value === 'string' && value.indexOf('->') === 0) {
				// Find the reference to the object
				return $this.findReference(value.substr(2));
			}

			return value;
		},
		/**
		 * This method searches for the _id associated with the object represented
		 * by the reference provided.
		 *
		 * @param  {String} ref The string representation of the reference.
		 * @return {String}	 The reference to the object.
		 */
		findReference: function (ref) {
			var keys = ref.split('.');
			var key = keys.shift();
			var result = $this.result[key];

			if (!result) {
				// If the result does not exist, return an empty
				throw new TypeError('Could not read property \'' + key + '\' from undefined');
			}

			// Iterate over all the keys and find the property
			while ((key = keys.shift())) {
				result = result[key];
			}

			return result;
		},

		log: function (args) {
			console.log(new Date().toISOString().slice(11,19), args);
		}
	};

	return {
		seed: function (dataFile, options) {
			$this.options = Object.assign(DEFAULT_OPTIONS, options);
			$this.sandbox = {};

			return $this.seed(path.resolve(process.cwd(), dataFile));
		},
		connect: function (options) {
			$this.Dynamo = options.service ? options.service : new AWS.DynamoDb(options);
			$this.dynamo = new AWS.DynamoDB.DocumentClient({service: $this.Dynamo});
		}
	};
})();
