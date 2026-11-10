const Test = require('thunk-test')
const assert = require('assert')
const DynamoStream = require('./DynamoStream')
const Dynamo = require('./Dynamo')
const ClaimyrFakeDynamoStreams = require('./ClaimyrFakeDynamoStreams')

// Claimyr's 0.29.18 change to DynamoStream: the shards are listed when the iteration starts, when a shard's reads end
// with no child known, and otherwise only on a slow safety timer, instead of ListStreams + DescribeStream every 15 s
// in every reader. A child shard is read after its parent. Each shard of the first listing may start at a sequence
// number, idle shards may poll less often, and close() ends every wait. Everything runs against an in-memory DynamoDB
// Streams (ClaimyrFakeDynamoStreams.js): no network.

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))

// a DynamoStream built by its own constructor, whose AWS calls answer from the fake
const FakeStream = function (fake, options = {}) {
  const describeTable = Dynamo.prototype.describeTable
  Dynamo.prototype.describeTable = async () => ({
    Table: { StreamSpecification: { StreamEnabled: true, StreamViewType: 'NEW_AND_OLD_IMAGES' } },
  })
  try {
    return fake.attach(new DynamoStream({
      table: fake.table, getRecordsInterval: 5, region: 'us-west-1', ...options,
    }))
  } finally {
    Dynamo.prototype.describeTable = describeTable
  }
}

// reads the stream in the background into `records`
const Reader = function (stream) {
  const reader = { records: [], error: null }
  reader.done = (async () => {
    try {
      for await (const record of stream) {
        reader.records.push(record)
      }
    } catch (error) {
      reader.error = error
    }
  })()
  return reader
}

const waitFor = async function (condition, ms = 3000) {
  const start = Date.now()
  while (!condition()) {
    if (Date.now() - start > ms) {
      throw new Error(`timed out waiting for ${condition}`)
    }
    await sleep(5)
  }
}

const ids = records => records.map(record => record.dynamodb.NewImage.id.S)
const ofShard = (records, shardId) => ids(records.filter(record => record.shardId == shardId))
const listings = fake => fake.calls.listStreams

const quietly = async function (f) {
  const saved = console.error
  const lines = []
  console.error = (...args) => lines.push(args.join(' '))
  try {
    return await f(lines)
  } finally {
    console.error = saved
  }
}

const test = new Test('Claimyr DynamoStream lists shards when one ends, reads a child after its parent', async function () {
  { // the defaults: a slow safety listing, no idle back-off
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_defaults' })
    const stream = FakeStream(fake, { getRecordsInterval: undefined })
    assert.equal(stream.shardUpdatePeriod, 300000)
    assert.equal(stream.getRecordsInterval, 1000)
    assert.equal(stream.idleGetRecordsInterval, 1000)
    assert.equal(DynamoStream.NO_OPEN_SHARD_PERIOD, 15000)
    const explicit = FakeStream(fake, { shardUpdatePeriod: 1000, idleGetRecordsInterval: 5000 })
    assert.equal(explicit.shardUpdatePeriod, 1000)
    assert.equal(explicit.idleGetRecordsInterval, 5000)
    // an idle interval below the read interval is the read interval
    assert.equal(FakeStream(fake, { getRecordsInterval: 2000, idleGetRecordsInterval: 500 }).idleGetRecordsInterval, 2000)
  }

  { // no listing while every shard stays open, however many reads (0.29.17 listed every 15 s)
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_steady' })
    fake.addShard('shardId-a')
    fake.addShard('shardId-b')
    fake.addShard('shardId-c')
    const stream = FakeStream(fake)
    const reader = Reader(stream)
    await waitFor(() => fake.calls.getShardIterator == 3)
    for (let i = 0; i < 30; i += 1) {
      fake.put(['shardId-a', 'shardId-b', 'shardId-c'][i % 3], { id: `r${i}` })
      await sleep(2)
    }
    await waitFor(() => reader.records.length == 30)
    await sleep(100)
    stream.close()
    await reader.done
    assert.equal(reader.error, null)
    assert.equal(listings(fake), 1)
    assert.equal(fake.calls.describeStream, 1)
    assert(fake.calls.getRecords > 30, `${fake.calls.getRecords} reads`)
    // each shard's records in the shard's order
    assert.deepEqual(ofShard(reader.records, 'shardId-a'), ['r0', 'r3', 'r6', 'r9', 'r12', 'r15', 'r18', 'r21', 'r24', 'r27'])
    assert.deepEqual(ofShard(reader.records, 'shardId-c'), ['r2', 'r5', 'r8', 'r11', 'r14', 'r17', 'r20', 'r23', 'r26', 'r29'])
    assert.equal(reader.records[0].table, 'test_steady')
  }

  { // a rollover: the parent closes, one listing finds its child, the child is read from its first record
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_rollover' })
    fake.addShard('shardId-a')
    fake.addShard('shardId-b')
    const stream = FakeStream(fake)
    const reader = Reader(stream)
    await waitFor(() => fake.calls.getShardIterator == 2)
    fake.put('shardId-a', { id: 'a1' })
    fake.put('shardId-a', { id: 'a2' })
    fake.close('shardId-a', ['shardId-a1'])
    fake.put('shardId-a1', { id: 'a3' }) // written to the child before the parent's last records are read
    fake.put('shardId-a1', { id: 'a4' })
    fake.put('shardId-b', { id: 'b1' })
    await waitFor(() => reader.records.length == 5)
    await sleep(50)
    stream.close()
    await reader.done
    assert.equal(listings(fake), 2) // the start, and the parent's end
    // the item's four writes in order across the rollover
    assert.deepEqual(ids(reader.records.filter(record => record.shardId.startsWith('shardId-a'))), ['a1', 'a2', 'a3', 'a4'])
    assert.deepEqual(ofShard(reader.records, 'shardId-a1'), ['a3', 'a4'])
  }

  { // a split: two children, both read after the parent; the next rollover lists once more
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_split' })
    fake.addShard('shardId-a')
    const stream = FakeStream(fake)
    const reader = Reader(stream)
    await waitFor(() => fake.calls.getShardIterator == 1)
    fake.put('shardId-a', { id: 'p1' })
    fake.close('shardId-a', ['shardId-x', 'shardId-y'])
    fake.put('shardId-x', { id: 'x1' })
    fake.put('shardId-y', { id: 'y1' })
    await waitFor(() => reader.records.length == 3)
    assert.equal(ids(reader.records)[0], 'p1')
    fake.close('shardId-x', ['shardId-x2'])
    fake.put('shardId-x2', { id: 'x2' })
    await waitFor(() => reader.records.length == 4)
    stream.close()
    await reader.done
    assert.equal(listings(fake), 3)
    assert.deepEqual(ofShard(reader.records, 'shardId-x2'), ['x2'])
  }

  { // the first listing holds a closed parent and its child: read from TRIM_HORIZON, the parent's records come first
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_lineage' })
    fake.addShard('shardId-old')
    for (let i = 0; i < 5; i += 1) fake.put('shardId-old', { id: `old${i}` })
    fake.close('shardId-old', ['shardId-mid'])
    for (let i = 0; i < 5; i += 1) fake.put('shardId-mid', { id: `mid${i}` })
    fake.close('shardId-mid', ['shardId-new'])
    for (let i = 0; i < 5; i += 1) fake.put('shardId-new', { id: `new${i}` })
    // the child listed before its parents, as DescribeStream may page them
    fake.current.shards.reverse()
    const stream = FakeStream(fake, { shardIteratorType: 'TRIM_HORIZON' })
    const reader = Reader(stream)
    await waitFor(() => reader.records.length == 15)
    stream.close()
    await reader.done
    assert.deepEqual(ids(reader.records), [
      'old0', 'old1', 'old2', 'old3', 'old4', 'mid0', 'mid1', 'mid2', 'mid3', 'mid4', 'new0', 'new1', 'new2', 'new3', 'new4',
    ])
    assert.equal(listings(fake), 1) // the closed shards' children were in the first listing
  }

  { // read from LATEST, a closed parent yields nothing and holds no child back
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_latest_closed' })
    fake.addShard('shardId-old')
    fake.put('shardId-old', { id: 'old0' })
    fake.close('shardId-old', ['shardId-new'])
    fake.put('shardId-new', { id: 'before-start' })
    const stream = FakeStream(fake)
    const reader = Reader(stream)
    await waitFor(() => fake.calls.getShardIterator == 2)
    fake.put('shardId-new', { id: 'after-start' })
    await waitFor(() => reader.records.length == 1)
    await sleep(50)
    stream.close()
    await reader.done
    assert.deepEqual(ids(reader.records), ['after-start'])
    assert.equal(listings(fake), 1)
  }

  { // a shard that ends with no child yet: listed again after 1 s, the child is read from its first record
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_late_child' })
    fake.addShard('shardId-a')
    const stream = FakeStream(fake)
    const reader = Reader(stream)
    await waitFor(() => fake.calls.getShardIterator == 1)
    fake.close('shardId-a') // DynamoDB has not listed the child yet
    await waitFor(() => listings(fake) == 2) // the end of shardId-a
    fake.addShard('shardId-b', 'shardId-a')
    fake.put('shardId-b', { id: 'b1' })
    await waitFor(() => reader.records.length == 1, 3000)
    stream.close()
    await reader.done
    assert.equal(listings(fake), 3)
    assert.deepEqual(ids(reader.records), ['b1'])
  }

  { // a disabled stream: every shard ends with no child; the listings stay few, and the new stream is read
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_disabled' })
    for (let i = 0; i < 6; i += 1) fake.addShard(`shardId-${i}`)
    const stream = FakeStream(fake)
    const reader = Reader(stream)
    await waitFor(() => fake.calls.getShardIterator == 6)
    fake.disableStream()
    fake.put('shardId-new-0', { id: 'n1' })
    await waitFor(() => reader.records.length == 1, 3000)
    // the six ended shards never get a child: listings back off (1, 2, 4 ... s) instead of one per shard
    await sleep(1500)
    stream.close()
    await reader.done
    assert.deepEqual(ids(reader.records), ['n1'])
    assert(listings(fake) <= 4, `${listings(fake)} listings`)
  }

  { // a caller's shardUpdatePeriod still lists on its own timer (services' tests pass 1000 for DynamoDB Local)
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_period' })
    fake.addShard('shardId-a')
    const stream = FakeStream(fake, { shardUpdatePeriod: 50 })
    const reader = Reader(stream)
    await waitFor(() => fake.calls.getShardIterator == 1)
    fake.addShard('shardId-b') // a shard that appears with no shard ending (DynamoDB Local's first shard)
    fake.put('shardId-b', { id: 'b1' })
    await waitFor(() => reader.records.length == 1)
    await sleep(200)
    stream.close()
    await reader.done
    assert(listings(fake) >= 4, `${listings(fake)} listings`)
  }

  { // startingShard: each shard of the first listing from its own place (AFTER_SEQUENCE_NUMBER), others from TRIM_HORIZON
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_positions' })
    fake.addShard('shardId-a')
    fake.addShard('shardId-b')
    fake.put('shardId-a', { id: 'a1' })
    const a2 = fake.put('shardId-a', { id: 'a2' })
    fake.put('shardId-a', { id: 'a3' })
    fake.put('shardId-b', { id: 'b1' })
    const stream = FakeStream(fake)
    stream.startingShard = Shard => Shard.ShardId == 'shardId-a'
      ? { ...Shard, ShardIteratorType: 'AFTER_SEQUENCE_NUMBER', SequenceNumber: a2.dynamodb.SequenceNumber }
      : { ...Shard, ShardIteratorType: 'TRIM_HORIZON' }
    const reader = Reader(stream)
    await waitFor(() => reader.records.length == 2)
    fake.put('shardId-a', { id: 'a4' })
    await waitFor(() => reader.records.length == 3)
    stream.close()
    await reader.done
    assert.deepEqual(ofShard(reader.records, 'shardId-a'), ['a3', 'a4'])
    assert.deepEqual(ofShard(reader.records, 'shardId-b'), ['b1'])
  }

  { // startingShard null: a shard with nothing left to read is skipped, and its child is not held back by it
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_skip' })
    fake.addShard('shardId-done')
    fake.put('shardId-done', { id: 'already-had' })
    fake.close('shardId-done', ['shardId-child'])
    fake.put('shardId-child', { id: 'c1' })
    const stream = FakeStream(fake, { getRecordsInterval: 60000 })
    stream.startingShard = Shard => Shard.ShardId == 'shardId-done' ? null : { ...Shard, ShardIteratorType: 'TRIM_HORIZON' }
    const reader = Reader(stream)
    await waitFor(() => reader.records.length == 1, 1000)
    stream.close()
    await reader.done
    assert.deepEqual(ids(reader.records), ['c1'])
    assert.equal(fake.getRecordsByShard.get('shardId-done'), undefined)
  }

  { // a lineage of closed shards is read without a read interval between them (each ends on its last read)
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_fast_lineage' })
    fake.addShard('shardId-g0')
    for (let g = 0; g < 6; g += 1) {
      fake.put(`shardId-g${g}`, { id: `g${g}` })
      fake.close(`shardId-g${g}`, [`shardId-g${g + 1}`])
    }
    fake.put('shardId-g6', { id: 'g6' })
    const stream = FakeStream(fake, { getRecordsInterval: 1000, shardIteratorType: 'TRIM_HORIZON' })
    const reader = Reader(stream)
    const started = Date.now()
    await waitFor(() => reader.records.length == 7, 1500)
    assert(Date.now() - started < 900, `${Date.now() - started} ms for seven generations`)
    stream.close()
    await reader.done
    assert.deepEqual(ids(reader.records), ['g0', 'g1', 'g2', 'g3', 'g4', 'g5', 'g6'])
  }

  { // a closed shard's pages are read 200 ms apart, not a read interval apart (a catch-up through closed shards)
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_closed_pages' })
    fake.addShard('shardId-old')
    for (let i = 0; i < 5; i += 1) fake.put('shardId-old', { id: `p${i}` })
    fake.close('shardId-old', ['shardId-new'])
    const stream = FakeStream(fake, { getRecordsInterval: 5000, getRecordsLimit: 1, shardIteratorType: 'TRIM_HORIZON' })
    const reader = Reader(stream)
    const started = Date.now()
    await waitFor(() => reader.records.length == 5, 2500)
    assert(Date.now() - started < 1500, `${Date.now() - started} ms for five pages`)
    stream.close()
    await reader.done
    assert.equal(DynamoStream.CLOSED_SHARD_INTERVAL, 200)
  }

  { // idle back-off: an idle shard waits twice as long after each empty read, a busy one keeps the read interval
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_idle' })
    fake.addShard('shardId-idle')
    fake.addShard('shardId-busy')
    const stream = FakeStream(fake, { getRecordsInterval: 10, idleGetRecordsInterval: 80 })
    const reader = Reader(stream)
    await waitFor(() => fake.calls.getShardIterator == 2)
    const started = Date.now()
    let n = 0
    while (Date.now() - started < 600) {
      fake.put('shardId-busy', { id: `busy${n}` })
      n += 1
      await sleep(8)
    }
    const idleReads = fake.getRecordsByShard.get('shardId-idle')
    const busyReads = fake.getRecordsByShard.get('shardId-busy')
    await waitFor(() => reader.records.length == n)
    stream.close()
    await reader.done
    // idle: 10, 20, 40, 80, 80 ... ms, about 10 reads in 600 ms; without the back-off it would be about 50
    assert(idleReads <= 14, `${idleReads} idle reads`)
    assert(busyReads >= 25, `${busyReads} busy reads`)
    assert.equal(ofShard(reader.records, 'shardId-busy').length, n)
  }

  { // the hooks: every listing and every read, and a hook that throws stops nothing
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_hooks' })
    fake.addShard('shardId-a')
    const seen = { shards: [], reads: 0, withRecords: 0 }
    const stream = FakeStream(fake, {
      onShards: shards => seen.shards.push(shards.map(Shard => Shard.ShardId)),
      onGetRecords: (Shard, response) => {
        seen.reads += 1
        if (response.Records.length > 0) {
          seen.withRecords += 1
          throw new Error('a hook that throws')
        }
      },
    })
    await quietly(async lines => {
      const reader = Reader(stream)
      await waitFor(() => fake.calls.getShardIterator == 1)
      fake.put('shardId-a', { id: 'a1' })
      fake.put('shardId-a', { id: 'a2' })
      await waitFor(() => reader.records.length == 2)
      stream.close()
      await reader.done
      assert(lines.some(line => line.includes('onGetRecords threw')))
    })
    assert.deepEqual(seen.shards, [['shardId-a']])
    assert(seen.reads >= 2)
    assert(seen.withRecords >= 1)
  }

  { // close() ends every wait at once, the 5-minute safety listing's included
    const fake = new ClaimyrFakeDynamoStreams({ table: 'test_close' })
    fake.addShard('shardId-a')
    const stream = FakeStream(fake, { getRecordsInterval: 60000 })
    const reader = Reader(stream)
    await waitFor(() => fake.calls.getRecords == 1)
    const closing = Date.now()
    stream.close()
    await reader.done
    assert(Date.now() - closing < 200, `${Date.now() - closing} ms to close`)
    assert.equal(stream.sleeps.size, 0)
  }
}).case()

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
