const Test = require('thunk-test')
const assert = require('assert')
const DynamoStream = require('./DynamoStream')

// Claimyr's change to DynamoStream: a shard iterator DynamoDB Streams refuses (TrimmedDataAccessException,
// ExpiredIteratorException) is replaced, and the shard reads on. Before it, a trimmed iterator threw out of the stream
// uncaught and stopped the service, and an expired one ended the shard's reads (or, on the first read, threw a
// TypeError). Nothing here opens a network connection: the shard reader runs against scripted AWS responses.

const AwsError = function (code, message) {
  const error = new Error(message)
  error.code = code
  return error
}
const trimmed = () => AwsError('TrimmedDataAccessException', 'The operation attempted to read past the oldest stream record in a shard.')
const expired = () => AwsError('ExpiredIteratorException', 'Iterator expired. The iterator was created at time ...')

const record = SequenceNumber => ({ eventName: 'INSERT', dynamodb: { SequenceNumber } })

// a DynamoStream whose GetShardIterator and GetRecords answer from a script, and which records what was asked
const ScriptedStream = function ({ shardIteratorType, getRecordsResponses }) {
  const stream = Object.create(DynamoStream.prototype)
  stream.table = 'test_table'
  stream.getRecordsLimit = 1000
  stream.getRecordsInterval = 0
  stream.shardIteratorRequests = []
  stream.getRecordsRequests = []
  stream.retryGetShardIterator = async params => {
    stream.shardIteratorRequests.push(params)
    return { ShardIterator: `iterator-${stream.shardIteratorRequests.length}` }
  }
  const responses = [...getRecordsResponses]
  stream.retryGetRecords = async params => {
    stream.getRecordsRequests.push(params.ShardIterator)
    const response = responses.shift()
    if (response instanceof Error) {
      throw response
    }
    return response
  }
  stream.shard = {
    ShardId: 'shardId-0001',
    Stream: { StreamArn: 'arn:aws:dynamodb:us-west-1:000000000000:table/test_table/stream/2026-10-13T00:00:00.000' },
    ShardIteratorType: shardIteratorType,
  }
  return stream
}

const readAll = async function (stream) {
  const records = []
  for await (const item of stream.getRecords(stream.shard)) {
    records.push(item)
  }
  return records
}

const sequenceNumbers = records => records.map(item => item.dynamodb.SequenceNumber)
const iteratorTypes = stream => stream.shardIteratorRequests.map(params => params.ShardIteratorType)

// collects DynamoStream's own console.error lines; anything else (the AWS SDK's notices) still prints
const quietly = async function (f) {
  const saved = console.error
  const lines = []
  console.error = (...args) => {
    const line = args.join(' ')
    if (line.startsWith('DynamoStream: ')) lines.push(line)
    else saved(...args)
  }
  try {
    await f()
  } finally {
    console.error = saved
  }
  return lines
}

const test = new Test('Claimyr DynamoStream replaces refused shard iterators', async function () {
  { // trimmed on the first read (a TRIM_HORIZON iterator whose records were trimmed before GetRecords used it)
    const stream = ScriptedStream({
      shardIteratorType: 'TRIM_HORIZON',
      getRecordsResponses: [
        trimmed(),
        { Records: [record('200'), record('201')], NextShardIterator: 'next-1' },
        { Records: [record('202')], NextShardIterator: null },
      ],
    })
    let records = null
    const lines = await quietly(async () => {
      records = await readAll(stream)
    })
    assert.deepEqual(sequenceNumbers(records), ['200', '201', '202'])
    assert.deepEqual(iteratorTypes(stream), ['TRIM_HORIZON', 'TRIM_HORIZON'])
    assert.deepEqual(stream.getRecordsRequests, ['iterator-1', 'iterator-2', 'next-1'])
    assert.equal(records[0].table, 'test_table')
    assert.equal(records[0].shardId, 'shardId-0001')
    assert.equal(lines.length, 1)
    assert(lines[0].includes('test_table shard shardId-0001: TrimmedDataAccessException'))
  }

  { // trimmed mid-stream: read on from the oldest record the shard still holds
    const stream = ScriptedStream({
      shardIteratorType: 'LATEST',
      getRecordsResponses: [
        { Records: [record('100')], NextShardIterator: 'next-1' },
        trimmed(),
        { Records: [record('300')], NextShardIterator: null },
      ],
    })
    let records = null
    await quietly(async () => {
      records = await readAll(stream)
    })
    assert.deepEqual(sequenceNumbers(records), ['100', '300'])
    assert.deepEqual(iteratorTypes(stream), ['LATEST', 'TRIM_HORIZON'])
  }

  { // expired after records were read: read on after the last one, so nothing is read twice or skipped
    const stream = ScriptedStream({
      shardIteratorType: 'TRIM_HORIZON',
      getRecordsResponses: [
        { Records: [record('100'), record('101')], NextShardIterator: 'next-1' },
        expired(),
        { Records: [record('102')], NextShardIterator: null },
      ],
    })
    let records = null
    await quietly(async () => {
      records = await readAll(stream)
    })
    assert.deepEqual(sequenceNumbers(records), ['100', '101', '102'])
    assert.deepEqual(iteratorTypes(stream), ['TRIM_HORIZON', 'AFTER_SEQUENCE_NUMBER'])
    assert.equal(stream.shardIteratorRequests[1].SequenceNumber, '101')
    assert.equal(stream.shardIteratorRequests[0].SequenceNumber, undefined)
  }

  { // expired, and the last record read has been trimmed as well: read from the oldest record the shard holds
    const stream = ScriptedStream({
      shardIteratorType: 'TRIM_HORIZON',
      getRecordsResponses: [
        { Records: [record('100')], NextShardIterator: 'next-1' },
        expired(),
        { Records: [record('500')], NextShardIterator: null },
      ],
    })
    const getShardIterator = stream.retryGetShardIterator
    stream.retryGetShardIterator = async params => {
      if (params.ShardIteratorType == 'AFTER_SEQUENCE_NUMBER') {
        stream.shardIteratorRequests.push(params)
        throw trimmed()
      }
      return getShardIterator(params)
    }
    let records = null
    await quietly(async () => {
      records = await readAll(stream)
    })
    assert.deepEqual(sequenceNumbers(records), ['100', '500'])
    assert.deepEqual(iteratorTypes(stream), ['TRIM_HORIZON', 'AFTER_SEQUENCE_NUMBER', 'TRIM_HORIZON'])
  }

  { // expired before any record on a LATEST shard: start at LATEST again rather than replay the shard
    const stream = ScriptedStream({
      shardIteratorType: 'LATEST',
      getRecordsResponses: [
        { Records: [], NextShardIterator: 'next-1' },
        AwsError('ExpiredIteratorException', 'Shard iterator has expired'),
        { Records: [record('700')], NextShardIterator: null },
      ],
    })
    let records = null
    await quietly(async () => {
      records = await readAll(stream)
    })
    assert.deepEqual(sequenceNumbers(records), ['700'])
    assert.deepEqual(iteratorTypes(stream), ['LATEST', 'LATEST'])
  }

  { // refused on every replacement: gives up after three and throws, as before
    const stream = ScriptedStream({
      shardIteratorType: 'TRIM_HORIZON',
      getRecordsResponses: [trimmed(), trimmed(), trimmed(), trimmed(), { Records: [], NextShardIterator: null }],
    })
    let thrown = null
    const lines = await quietly(async () => {
      await readAll(stream).catch(error => {
        thrown = error
      })
    })
    assert.equal(thrown && thrown.code, 'TrimmedDataAccessException')
    assert.equal(stream.getRecordsRequests.length, 4)
    assert.equal(lines.length, 3)
  }

  { // any other error still throws at once
    const stream = ScriptedStream({
      shardIteratorType: 'TRIM_HORIZON',
      getRecordsResponses: [AwsError('ResourceNotFoundException', 'Requested resource not found')],
    })
    await assert.rejects(readAll(stream), { code: 'ResourceNotFoundException' })
    assert.deepEqual(iteratorTypes(stream), ['TRIM_HORIZON'])
  }
}).case()

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
