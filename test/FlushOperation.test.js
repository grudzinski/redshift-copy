var FlushOperation = require('../lib/FlushOperation.js');
var PolyMock = require('polymock');
var testutil = require('./lib/testutil.js');
var assert = require('assert');

describe('FlushOperation', function () {
	var topic = FlushOperation.prototype;

	function createFlushOperationMock() {
		var mock = PolyMock.create();

		mock.createProperty('key', testutil.EXPECTED_KEY);
		mock.createProperty('copyQuery', testutil.EXPECTED_COPY_QUERY);

		mock.b1 = new Buffer('1, 2, 3');
		mock.b2 = new Buffer('1, 2, 3');

		mock.createProperty('buffer', [mock.b1, mock.b2]);
		mock.createProperty('bufferLength', mock.b1.length + mock.b2.length);

		// the following methods of FlushOperation are function generators.
		// special() is a method that return a function as expected. The returned function is also added to the mock with the same
		// name as the generator function but with a suffix of Functor, e.g _uploadToS3Functor.
		// This way when the test calls _uploadToS3 mock, it returns a function that is a mock as well.
		//
		// TODO this is actually too complex, and must be implemented differently
		mock.createMethod('_uploadToS3', special('_uploadToS3'));
		mock.createMethod('_updateUploadLatency', special('_updateUploadLatency'));
		mock.createMethod('_updateQueryLatency', special('_updateQueryLatency'));
		mock.createMethod('_executeCopyQuery', special('_executeCopyQuery'));

		mock.createMethod('_prepareBuffer', Buffer.concat([mock.b1, mock.b2]));
		mock.createMethod('done');
		mock.createMethod('emit');

		return mock;

		// creates a functor on the mock object that records invocations of returned functions
		function special(what) {
			var name = what + 'Functor';

			mock.createMethod(name, undefined);

			return function() {
				mock.object[name].apply(mock.object, Array.prototype.slice(arguments, 0));
			}
		}
	}

	function createS3Mock() {
		var mock = PolyMock.create();

		mock.createMethod('put', undefined, { callbackArgs: [null, { statusCode: 200} ] });

		return mock;
	}

	it('Ctor', function () {

		var expectedBuffer = [new Buffer('1'), new Buffer('2')];
		var flushOp = new FlushOperation('mybucket', 'mykey', expectedBuffer, Buffer.concat(expectedBuffer).length, testutil.EXPECTED_COPY_QUERY);

		assert.strictEqual(flushOp.bucket, 'mybucket');
		assert.strictEqual(flushOp.key, 'mykey');
		assert.deepEqual(flushOp.buffer, expectedBuffer);
		assert.strictEqual(flushOp.bufferLength, 2);
		assert.strictEqual(flushOp.copyQuery, testutil.EXPECTED_COPY_QUERY);
	});

	it('uploads to s3', function () {

		var expectedBuffer = [new Buffer('1'), new Buffer('2')];
		var flushOp = new FlushOperation('mybucket', 'mykey', expectedBuffer, Buffer.concat(expectedBuffer).length, testutil.EXPECTED_COPY_QUERY);
		var s3mock = createS3Mock();

		var functor = flushOp._uploadToS3(s3mock.object);

		// call the functor
		functor(function(err, res) {});

		assert.strictEqual(s3mock.invocations[0].method, 'put');
		assert.strictEqual(s3mock.invocations[0].arguments[0], 'mykey');
		assert.deepEqual(s3mock.invocations[0].arguments[1], Buffer.concat(expectedBuffer));
		assert.strictEqual(typeof(s3mock.invocations[0].arguments[2]), 'function');
	});

	it('executes a copy query', function () {

		var datastoreMock = testutil.createDatastoreMock();

		var expectedBuffer = [new Buffer('1'), new Buffer('2')];
		var flushOp = new FlushOperation('mybucket', 'mykey', expectedBuffer, Buffer.concat(expectedBuffer).length, testutil.EXPECTED_COPY_QUERY);

		var functor = flushOp._executeCopyQuery(datastoreMock.object);

		functor('123', function(err, results) {});

		assert.strictEqual(datastoreMock.invocations[0].method, 'query');
		assert.strictEqual(datastoreMock.invocations[0].arguments[0], testutil.EXPECTED_COPY_QUERY);
	});

	it('executes an async flow when started', function (done) {
		var expectedBuffer = [new Buffer('1'), new Buffer('2')];
		var flushOp = new FlushOperation('mybucket', 'mykey', expectedBuffer, Buffer.concat(expectedBuffer).length, testutil.EXPECTED_COPY_QUERY);

		var dsMock = testutil.createDatastoreMock();
		var s3Mock = createS3Mock();

		flushOp.start(s3Mock.object, dsMock.object);

		flushOp.on('success', function (flushOp) {
			assert.strictEqual(flushOp.copyQuery, 'copy 123');
			assert.strictEqual(flushOp.bucket, 'mybucket');
			assert.strictEqual(flushOp.key, 'mykey');
			assert.strictEqual(flushOp.buffer.length, 2);
			assert.deepEqual(flushOp.buffer, expectedBuffer);
			assert.strictEqual(flushOp.stage, '_executeCopyQuery');

			// this is not the length of the array but the sum of the lengths
			// of all the buffers in the array
			assert.strictEqual(flushOp.bufferLength, 2);
			done()
		})

		flushOp.on('error', done);
	});
});