var PolyMock = require('polymock');

module.exports.EXPECTED_FILENAME = 'lala.log';
module.exports.EXPECTED_KEY = 'test/' + module.exports.EXPECTED_FILENAME;
module.exports.EXPECTED_COPY_QUERY = 'copy 123';
module.exports.EXPECTED_BUCKET = 'mybucket';

module.exports.createDatastoreMock = function() {
	var mock = PolyMock.create();

	mock.createMethod('query', undefined, { callbackArgs: [null, []] });

	return mock;
}

