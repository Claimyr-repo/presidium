/**
 * @name ClaimyrFakeDynamoStreams
 *
 * @synopsis
 * ```coffeescript [specscript]
 * new ClaimyrFakeDynamoStreams({ table: string, now?: ()=>number }) -> fake
 *
 * fake.addShard(ShardId string, ParentShardId?: string) -> ()
 * fake.put(ShardId string, item object, eventName?: 'INSERT'|'MODIFY'|'REMOVE') -> record // the record as AWS returns it
 * fake.close(ShardId string, children?: Array<string>) -> () // closes a shard, opens its children
 * fake.disableStream() -> () // closes every shard with no child, opens a new stream with one shard 'shardId-new-0'
 * fake.attach(stream DynamoStream, label?: string) -> stream // the stream's four AWS calls answer from this fake
 * fake.calls -> { listStreams: number, describeStream: number, getShardIterator: number, getRecords: number }
 * fake.callsBy -> Map<label, { listStreams, describeStream, getShardIterator, getRecords }> // per attached stream
 * fake.getRecordsByShard -> Map<ShardId, number>
 * fake.hold(label string) -> () // that stream's GetRecords wait until fake.release(label)
 * ```
 *
 * @description
 * An in-memory DynamoDB Streams for tests: ListStreams, DescribeStream (paged, 100 shards a page by default),
 * GetShardIterator (TRIM_HORIZON, LATEST, AT_ and AFTER_SEQUENCE_NUMBER) and GetRecords, answering as AWS does in what
 * presidium's DynamoStream reads: a closed shard's GetRecords answers `NextShardIterator: null` once every record in it
 * was read, sequence numbers grow across the table as decimal strings of one length, and a record's
 * `ApproximateCreationDateTime` is a Date rounded down to the second. No network, no timers.
 */
const ClaimyrFakeDynamoStreams = function ({ table, now = Date.now, pageSize = 100 }) {
  this.table = table
  this.now = now
  this.pageSize = pageSize
  this.sequence = 100000000000000000000n
  this.streams = []
  this.calls = { listStreams: 0, describeStream: 0, getShardIterator: 0, getRecords: 0 }
  this.callsBy = new Map()
  this.held = new Map()
  this.getRecordsByShard = new Map()
  this.openStream('2026-11-07T00:00:00.000')
}

ClaimyrFakeDynamoStreams.prototype.openStream = function (StreamLabel) {
  const stream = {
    StreamArn: `arn:aws:dynamodb:us-west-1:000000000000:table/${this.table}/stream/${StreamLabel}`,
    StreamLabel,
    TableName: this.table,
    shards: [],
  }
  this.streams.push(stream)
  this.current = stream
  return stream
}

ClaimyrFakeDynamoStreams.prototype.nextSequenceNumber = function () {
  this.sequence += 1n
  return String(this.sequence)
}

ClaimyrFakeDynamoStreams.prototype.shard = function (ShardId) {
  for (const stream of this.streams) {
    const shard = stream.shards.find(shard => shard.ShardId == ShardId)
    if (shard != null) {
      return shard
    }
  }
  throw new Error(`no shard ${ShardId}`)
}

ClaimyrFakeDynamoStreams.prototype.addShard = function (ShardId, ParentShardId) {
  this.current.shards.push({
    ShardId,
    ParentShardId,
    stream: this.current,
    records: [],
    StartingSequenceNumber: this.nextSequenceNumber(),
    EndingSequenceNumber: null,
  })
}

ClaimyrFakeDynamoStreams.prototype.put = function (ShardId, item, eventName = 'INSERT') {
  const shard = this.shard(ShardId)
  if (shard.EndingSequenceNumber != null) {
    throw new Error(`shard ${ShardId} is closed`)
  }
  const SequenceNumber = this.nextSequenceNumber()
  const image = {}
  for (const key in item) {
    const value = item[key]
    image[key] = Buffer.isBuffer(value) ? { B: value }
      : typeof value == 'number' ? { N: String(value) }
      : typeof value == 'boolean' ? { BOOL: value }
      : { S: String(value) }
  }
  const record = {
    eventID: `event-${SequenceNumber.slice(-8)}`,
    eventName,
    eventVersion: '1.1',
    eventSource: 'aws:dynamodb',
    awsRegion: 'us-west-1',
    dynamodb: {
      ApproximateCreationDateTime: new Date(Math.floor(this.now() / 1000) * 1000),
      Keys: { id: image.id },
      NewImage: image,
      SequenceNumber,
      SizeBytes: JSON.stringify(image).length,
      StreamViewType: 'NEW_AND_OLD_IMAGES',
    },
  }
  shard.records.push(record)
  return record
}

ClaimyrFakeDynamoStreams.prototype.close = function (ShardId, children = []) {
  const shard = this.shard(ShardId)
  shard.EndingSequenceNumber = shard.records.length > 0
    ? shard.records[shard.records.length - 1].dynamodb.SequenceNumber
    : shard.StartingSequenceNumber
  const saved = this.current
  this.current = shard.stream
  for (const child of children) {
    this.addShard(child, ShardId)
  }
  this.current = saved
}

ClaimyrFakeDynamoStreams.prototype.disableStream = function () {
  for (const shard of this.current.shards) {
    if (shard.EndingSequenceNumber == null) {
      this.close(shard.ShardId)
    }
  }
  this.openStream('2026-11-07T12:00:00.000')
  this.addShard('shardId-new-0')
}

const shardDescription = shard => ({
  ShardId: shard.ShardId,
  ...shard.ParentShardId == null ? {} : { ParentShardId: shard.ParentShardId },
  SequenceNumberRange: {
    StartingSequenceNumber: shard.StartingSequenceNumber,
    ...shard.EndingSequenceNumber == null ? {} : { EndingSequenceNumber: shard.EndingSequenceNumber },
  },
})

ClaimyrFakeDynamoStreams.prototype.listStreams = async function ({ TableName, ExclusiveStartStreamArn }) {
  this.calls.listStreams += 1
  const streams = this.streams.filter(stream => stream.TableName == TableName)
  const start = ExclusiveStartStreamArn == null ? 0
    : streams.findIndex(stream => stream.StreamArn == ExclusiveStartStreamArn) + 1
  return {
    Streams: streams.slice(start).map(stream => ({
      StreamArn: stream.StreamArn, StreamLabel: stream.StreamLabel, TableName: stream.TableName,
    })),
  }
}

ClaimyrFakeDynamoStreams.prototype.describeStream = async function ({ StreamArn, Limit, ExclusiveStartShardId }) {
  this.calls.describeStream += 1
  const stream = this.streams.find(stream => stream.StreamArn == StreamArn)
  const shards = stream.shards
  const start = ExclusiveStartShardId == null ? 0
    : shards.findIndex(shard => shard.ShardId == ExclusiveStartShardId) + 1
  const pageSize = Math.min(Limit ?? this.pageSize, this.pageSize)
  const page = shards.slice(start, start + pageSize)
  return {
    StreamDescription: {
      StreamArn,
      StreamStatus: 'ENABLED',
      Shards: page.map(shardDescription),
      ...start + pageSize < shards.length ? { LastEvaluatedShardId: page[page.length - 1].ShardId } : {},
    },
  }
}

ClaimyrFakeDynamoStreams.prototype.getShardIterator = async function ({ ShardId, ShardIteratorType, SequenceNumber }) {
  this.calls.getShardIterator += 1
  const shard = this.shard(ShardId)
  let position = null
  if (ShardIteratorType == 'TRIM_HORIZON') {
    position = 0
  } else if (ShardIteratorType == 'LATEST') {
    position = shard.records.length
  } else {
    const index = shard.records.findIndex(record => record.dynamodb.SequenceNumber == SequenceNumber)
    if (index == -1) {
      const error = new Error(`no record ${SequenceNumber} in ${ShardId}`)
      error.code = 'ValidationException'
      throw error
    }
    position = ShardIteratorType == 'AT_SEQUENCE_NUMBER' ? index : index + 1
  }
  return { ShardIterator: `${ShardId}|${position}` }
}

ClaimyrFakeDynamoStreams.prototype.getRecords = async function ({ ShardIterator, Limit }) {
  this.calls.getRecords += 1
  const [ShardId, at] = ShardIterator.split('|')
  this.getRecordsByShard.set(ShardId, (this.getRecordsByShard.get(ShardId) ?? 0) + 1)
  const shard = this.shard(ShardId)
  const position = Number(at)
  const Records = shard.records.slice(position, position + (Limit ?? 1000))
  const next = position + Records.length
  const closedAndRead = shard.EndingSequenceNumber != null && next >= shard.records.length
  return {
    Records: Records.map(record => ({ ...record, dynamodb: { ...record.dynamodb } })),
    NextShardIterator: closedAndRead ? null : `${ShardId}|${next}`,
  }
}

ClaimyrFakeDynamoStreams.prototype.attach = function (stream, label = 'unlabelled') {
  if (!this.callsBy.has(label)) {
    this.callsBy.set(label, { listStreams: 0, describeStream: 0, getShardIterator: 0, getRecords: 0 })
  }
  const count = name => { this.callsBy.get(label)[name] += 1 }
  stream.retryListStreams = params => (count('listStreams'), this.listStreams(params))
  stream.retryDescribeStream = params => (count('describeStream'), this.describeStream(params))
  stream.retryGetShardIterator = params => (count('getShardIterator'), this.getShardIterator(params))
  stream.retryGetRecords = async params => {
    count('getRecords')
    while (this.held.has(label)) {
      await this.held.get(label)
    }
    return this.getRecords(params)
  }
  return stream
}

ClaimyrFakeDynamoStreams.prototype.hold = function (label) {
  if (!this.held.has(label)) {
    let release = null
    const held = new Promise(resolve => { release = resolve })
    held.release = release
    this.held.set(label, held)
  }
}

ClaimyrFakeDynamoStreams.prototype.release = function (label) {
  const held = this.held.get(label)
  if (held != null) {
    this.held.delete(label)
    held.release()
  }
}

module.exports = ClaimyrFakeDynamoStreams
