var Monitor = require('../lib/Monitor.js')
var assert = require('assert')
var PolyMock = require('polymock')

var mockConfig = {
	flushLatencyThreshold: 2,
	activeFlushOperationsThreshold: 2,
	queryLatencyThreshold: 2,
	uploadLatencyThreshold: 2
}

function createFlushOpMock() {
	var mock = PolyMock.create()

	mock.createProperty('uploadLatency', 1)
	mock.createProperty('queryLatency', 1)

	return mock
}

function createCopyOpMock() {
	var mock = PolyMock.create()

	mock.createProperty('activeFlushOperations', 2)

	return mock
}

function createMonitorClientMock() {
	var mock = PolyMock.create()

	mock.createMethod('send')

	return mock
}

describe('Monitor', function () {

	var flushOp, copyOp, monitorClient, topic, event

	beforeEach(function () {
		flushOp = createFlushOpMock()
		copyOp = createCopyOpMock()
		monitorClient = createMonitorClientMock()
		topic = new Monitor(monitorClient.object, mockConfig)
		event = {
			name: 'some_event',
			ttl: 1
		}
	})

	it('monitors flush latency', function () {
		var listener = topic.flushLatencyListener(event)

		listener(flushOp.object)

		assert.strictEqual(monitorClient.invocations[0].method, 'send')

		var actual = monitorClient.invocations[0].arguments[0]

		assert.strictEqual(actual.service, 'flush latency')
		assert.strictEqual(actual.metric, 2)
		assert.deepEqual(actual.tags, ['performance', 'database', 'some_event'])
	})

	it('monitors active flush operations', function () {
		var listener = topic.activeFlushOperationsListener(copyOp.object, event)

		listener(flushOp.object)

		assert.strictEqual(monitorClient.invocations[0].method, 'send')

		var actual = monitorClient.invocations[0].arguments[0]

		assert.strictEqual(actual.service, 'concurrent flush operations')
		assert.strictEqual(actual.metric, 2)
		assert.deepEqual(actual.tags, ['performance', 'database', 'some_event'])
	})

	it('monitors copy query latency', function () {
		var listener = topic.copyQueryLatencyListener(event)

		listener(flushOp.object)

		assert.strictEqual(monitorClient.invocations[0].method, 'send')

		var actual = monitorClient.invocations[0].arguments[0]

		assert.strictEqual(actual.service, 'copy query latency')
		assert.strictEqual(actual.metric, 1)
		assert.deepEqual(actual.tags, ['performance', 'database', 'some_event'])
	})

	it('monitors upload latency', function () {
		var listener = topic.uploadLatencyListener(event)

		listener(flushOp.object)

		assert.strictEqual(monitorClient.invocations[0].method, 'send')

		var actual = monitorClient.invocations[0].arguments[0]

		assert.strictEqual(actual.service, 's3 upload latency')
		assert.strictEqual(actual.metric, 1)
		assert.deepEqual(actual.tags, ['performance', 'database', 'some_event'])
	})
})

