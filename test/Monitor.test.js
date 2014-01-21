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

	describe('monitors flush latency', function () {

		it('sends an event', function () {
			var listener = topic.flushLatencyListener(event)

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 'flush latency')
			assert.strictEqual(actual.metric, 2)
			assert.strictEqual(actual.state, 'ok')
			assert.deepEqual(actual.tags, ['performance', 'database', 'some_event'])
		})

		it('reports warning state when flush latency is higher than the threshold', function () {
			var listener = topic.flushLatencyListener(event)

			topic._config.flushLatencyThreshold = 1

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 'flush latency')
			assert.strictEqual(actual.state, 'warning')
		})

		it('reports critical state when flush latency is 3 times higher than the threshold', function () {
			var listener = topic.flushLatencyListener(event)

			topic._config.flushLatencyThreshold = 0.2

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 'flush latency')
			assert.strictEqual(actual.state, 'critical')
		})
	})

	describe('monitors active flush operations', function () {

		it('sends an event', function () {
			var listener = topic.activeFlushOperationsListener(copyOp.object, event)

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 'concurrent flush operations')
			assert.strictEqual(actual.metric, 2)
			assert.deepEqual(actual.tags, ['performance', 'database', 'some_event'])
		})

		it('reports warning state when active flush ops are higher than the threshold', function () {
			var listener = topic.activeFlushOperationsListener(copyOp.object, event)

			topic._config.activeFlushOpsThreshold = 1

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 'concurrent flush operations')
			assert.strictEqual(actual.state, 'warning')
		})

		it('reports critical state when active flush ops are 3 times higher than the threshold', function () {
			var listener = topic.activeFlushOperationsListener(copyOp.object, event)

			topic._config.activeFlushOpsThreshold = 0.2

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 'concurrent flush operations')
			assert.strictEqual(actual.state, 'critical')
		})
	})

	describe('monitors copy query latency', function () {
		it('sends an event', function () {
			var listener = topic.copyQueryLatencyListener(event)

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 'copy query latency')
			assert.strictEqual  (actual.metric, 1)
			assert.deepEqual(actual.tags, ['performance', 'database', 'some_event'])
		})

		it('reports warning state when copy query latency is higher than the threshold', function () {
			var listener = topic.copyQueryLatencyListener(event)

			topic._config.queryLatencyThreshold = 0.5

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 'copy query latency')
			assert.strictEqual(actual.state, 'warning')
		})

		it('reports critical state when copy query latency is 3 times higher than the threshold', function () {
			var listener = topic.copyQueryLatencyListener(event)

			topic._config.queryLatencyThreshold = 0.2

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 'copy query latency')
			assert.strictEqual(actual.state, 'critical')
		})
	})

	describe('monitors upload latency', function () {
		it('sends an event', function () {
			var listener = topic.uploadLatencyListener(event)

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 's3 upload latency')
			assert.strictEqual(actual.metric, 1)
			assert.deepEqual(actual.tags, ['performance', 'database', 'some_event'])
		})

		it('reports warning state when copy query latency is higher than the threshold', function () {
			var listener = topic.uploadLatencyListener(event)

			topic._config.uploadLatencyThreshold = 0.5

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 's3 upload latency')
			assert.strictEqual(actual.state, 'warning')
		})

		it('reports critical state when copy query latency is 3 times higher than the threshold', function () {
			var listener = topic.uploadLatencyListener(event)

			topic._config.uploadLatencyThreshold = 0.2

			listener(flushOp.object)

			assert.strictEqual(monitorClient.invocations[0].method, 'send')

			var actual = monitorClient.invocations[0].arguments[0]

			assert.strictEqual(actual.service, 's3 upload latency')
			assert.strictEqual(actual.state, 'critical')
		})
	})
})

