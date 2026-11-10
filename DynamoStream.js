require('rubico/global')
const Transducer = require('rubico/Transducer')
const DynamoDBStreams = require('aws-sdk/clients/dynamodbstreams')
const rubicox = require('rubico/x')
const has = require('./internal/has')
const RetryAwsErrors = require('./internal/RetryAwsErrors')
const HttpAgent = require('./HttpAgent')
const Dynamo = require('./Dynamo')
const placeholderCredentials = require('./internal/placeholderCredentials')
const Mux = require('rubico/monad/Mux')

const { identity, differenceWith } = rubicox

// ms between two safety listings of a stream's shards (up to 0.29.17: 15000, the only way a new shard was found)
const DEFAULT_SHARD_UPDATE_PERIOD = 300000

// ms between two listings while no open shard is known (a new table's stream before its first write)
const NO_OPEN_SHARD_PERIOD = 15000

// a child of a shard that ended is looked for again after 1, 2, 4, 8 and 16 s, then only by the safety listing
const CHILD_LISTING_RETRIES = 5

// ms a shard waits for its parent's reads to end before it is read anyway
const PARENT_WAIT = 60000

// ms between two reads of a shard that was closed when listed (DynamoDB Streams allows five reads a second per shard)
const CLOSED_SHARD_INTERVAL = 200

/**
 * @name DynamoStream
 *
 * @synopsis
 * ```coffeescript [specscript]
 * DynamoStream(options {
 *   table: string,
 *   accessKeyId: string,
 *   secretAccessKey: string,
 *   region: string,
 *   endpoint: string,
 *   streamViewType?: 'NEW_AND_OLD_IMAGES'|'NEW_IMAGE'|'OLD_IMAGE'|'KEYS_ONLY',
 *   shardIteratorType?: 'TRIM_HORIZON'|'LATEST'|'AT_SEQUENCE_NUMBER'|'AFTER_SEQUENCE_NUMBER',
 *   getRecordsInterval?: number, // ms between two GetRecords on a shard, 1000
 *   idleGetRecordsInterval?: number, // ms, the longest wait on a shard whose reads return nothing, getRecordsInterval
 *   shardUpdatePeriod?: number, // ms between two safety listings of the shards, 300000
 *   onShards?: Array<Shard>=>(), // each listing of the stream's shards
 *   onGetRecords?: (Shard, GetRecordsResponse)=>(), // each GetRecords answer
 * })
 * ```
 *
 * @description
 * https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDBStreams.html
 *
 * DynamoStream -> Stream Headers -> Stream ARNs
 *
 * `eventName` enumeration: `INSERT`, `MODIFY`, `REMOVE`
 *
 * Claimyr 0.29.18: the shards are listed (ListStreams, then DescribeStream page by page) when the iteration starts, again
 * when a shard's reads end (the shard closed and every record in it was read) and no child of it is known yet, and
 * otherwise only every `shardUpdatePeriod` as a safety net. Up to 0.29.17 every reader listed every 15 s, which on
 * 2026-11-07 was about 22 ListStreams and DescribeStream calls a second from Claimyr's services: management events that
 * GuardDuty bills, to find a new shard that DynamoDB opens only when another one closes. A shard that closes (every few
 * hours, or when a partition splits) is followed by a listing within one read of its end, so a new shard is read sooner
 * than before (up to 15 s then). While no open shard is known (a new table's stream before its first write), the
 * listing repeats every `min(shardUpdatePeriod, 15000)` ms.
 *
 * A shard whose parent this stream is still reading is read once the parent's reads end, so the records of one item
 * keep their order across a shard rollover. A closed parent read from LATEST yields nothing and holds no child back.
 *
 * `startingShard(Shard)` says where each shard of the first listing is read from: `this.shardIteratorType`, or a
 * caller's override, which may give a shard `ShardIteratorType: 'AFTER_SEQUENCE_NUMBER'` (or `AT_`) with its
 * `SequenceNumber`, or null for a shard that holds nothing left to read. Shards found later are read from
 * TRIM_HORIZON, as before. A shard read to its end (closed, its last record read) ends without a last read interval,
 * and a shard that was closed when listed is read 200 ms apart at most: it gets no new records, so catching up through
 * a lineage of closed shards (a TRIM_HORIZON start, a resume) reaches the open shard in seconds.
 *
 * `idleGetRecordsInterval` above `getRecordsInterval` lets a shard whose reads return nothing wait twice as long after
 * each empty read, up to that many ms, and back to `getRecordsInterval` after a read with records. Left unset, every
 * read waits `getRecordsInterval`, as before.
 *
 * `close()` ends every wait at once, so a closed stream no longer keeps a process alive.
 */

const DynamoStream = function (options) {
  if (this == null || this.constructor != DynamoStream) {
    return new DynamoStream(options)
  }
  const awsCreds = pick(options, [
    'accessKeyId',
    'secretAccessKey',
    'sessionToken',
    'region',
    'endpoint',
  ])
  this.table = options.table
  this.getRecordsLimit = options.getRecordsLimit ?? 1000
  this.getRecordsInterval = options.getRecordsInterval ?? 1000
  this.shardIteratorType = options.shardIteratorType ?? 'LATEST'
  this.idleGetRecordsInterval = Math.max(
    options.idleGetRecordsInterval ?? this.getRecordsInterval,
    this.getRecordsInterval,
  )
  this.shardUpdatePeriod = options.shardUpdatePeriod ?? DEFAULT_SHARD_UPDATE_PERIOD
  this.listStreamsLimit = options.listStreamsLimit ?? 100
  this.onShards = options.onShards
  this.onGetRecords = options.onGetRecords
  this.client = new DynamoDBStreams({
    apiVersion: '2012-08-10',
    ...placeholderCredentials(awsCreds),
    ...awsCreds,
  })

  this.retryListStreams = RetryAwsErrors(
    this.client.listStreams,
    this.client,
  )
  this.retryDescribeStream = RetryAwsErrors(
    this.client.describeStream,
    this.client,
  )
  this.retryGetShardIterator = RetryAwsErrors(
    this.client.getShardIterator,
    this.client,
  )
  this.retryGetRecords = RetryAwsErrors(
    this.client.getRecords,
    this.client,
  )

  const dynamo = new Dynamo(awsCreds)
  this.ready = dynamo.describeTable(this.table).then(pipe([
    get('Table.StreamSpecification'),
    streamSpec => streamSpec == null ? dynamo.enableStreams(this.table, {
      streamViewType: get('streamViewType', 'NEW_AND_OLD_IMAGES')(options)
    }) : {},
  ]))

  return this
}

// () => ()
DynamoStream.prototype.close = function close() {
  this.closed = true
  if (this.sleeps != null) {
    for (const sleep of this.sleeps) {
      clearTimeout(sleep.timer)
      sleep.resolve()
    }
    this.sleeps.clear()
  }
}

// sleep(ms number) -> Promise<>, a wait that close() ends at once
DynamoStream.prototype.sleep = function sleep(ms) {
  if (this.closed) {
    return Promise.resolve()
  }
  if (this.sleeps == null) {
    this.sleeps = new Set()
  }
  return new Promise(resolve => {
    const sleep = { resolve }
    sleep.timer = setTimeout(() => {
      this.sleeps.delete(sleep)
      resolve()
    }, ms)
    this.sleeps.add(sleep)
  })
}

// hook(name string, ...args) -> (), calls an onShards / onGetRecords option; a hook that throws is logged, never fatal
DynamoStream.prototype.hook = function hook(name, ...args) {
  if (typeof this[name] != 'function') {
    return
  }
  try {
    this[name](...args)
  } catch (error) {
    console.error(`DynamoStream: ${this.table}: ${name} threw`, error)
  }
}

// () => AsyncGenerator<Stream>
DynamoStream.prototype.getStreams = async function* getStreams() {
  let streams = await this.retryListStreams({
    Limit: this.listStreamsLimit,
    TableName: this.table
  })
  if (streams.Streams.length > 0) {
    yield* streams.Streams
  }
  while (!this.closed && streams.LastEvaluatedStreamArn != null) {
    streams = await this.retryListStreams({
      Limit: this.listStreamsLimit,
      TableName: this.table,
      ExclusiveStartStreamArn: streams.LastEvaluatedStreamArn,
    })
    if (streams.Streams.length > 0) {
      yield* streams.Streams
    }
  }
}

// Stream => AsyncGenerator<Shard>
DynamoStream.prototype.getShards = async function* getShards(
  Stream,
) {
  let shards = await this.retryDescribeStream({
    StreamArn: Stream.StreamArn,
    Limit: 100,
  }).then(get('StreamDescription'))
  if (shards.Shards.length > 0) {
    yield* shards.Shards.map(assign({ Stream: always(Stream) }))
  }
  while (!this.closed && shards.LastEvaluatedShardId != null) {
    shards = await this.retryDescribeStream({
      StreamArn: Stream.StreamArn,
      Limit: 100,
      ExclusiveStartShardId: shards.LastEvaluatedShardId,
    }).then(get('StreamDescription'))
    if (shards.Shards.length > 0) {
      yield* shards.Shards.map(assign({ Stream: always(Stream) }))
    }
  }
}

// handleGetRecordsError(error Error) -> ()
// Kept for callers; getRecords no longer uses it (it returned [] for an expired iterator, which ended the shard's reads)
const handleGetRecordsError = error => {
  if (error.message.includes('Shard iterator has expired')) {
    console.error(error)
    return []
  }
  throw error
}
DynamoStream.handleGetRecordsError = handleGetRecordsError

// DynamoDB Streams refuses a shard iterator whose records were trimmed (past the 24-hour retention, or trimmed between
// GetShardIterator and GetRecords) or that expired (unused for 15 minutes)
const isTrimmedError = error => error.code == 'TrimmedDataAccessException'
const isExpiredError = error => error.code == 'ExpiredIteratorException'
  || String(error.message).includes('Shard iterator has expired')

// how many times one read replaces a refused shard iterator before it gives up
const SHARD_ITERATOR_REPLACEMENTS = 3

// Shard => AsyncGenerator<Record>
DynamoStream.prototype.getRecords = async function* getRecords(
  Shard,
) {
  // a shard read from after a known record (AFTER_SEQUENCE_NUMBER) reads on after it if its iterator expires
  let lastSequenceNumber = Shard.ShardIteratorType == 'AFTER_SEQUENCE_NUMBER'
    ? Shard.SequenceNumber ?? null
    : null

  const getShardIterator = (ShardIteratorType, SequenceNumber) => this.retryGetShardIterator({
    ShardId: Shard.ShardId,
    StreamArn: Shard.Stream.StreamArn,
    ShardIteratorType,
    ...SequenceNumber == null ? {} : { SequenceNumber },
  }).then(get('ShardIterator'))

  // An iterator to replace one the stream refused. After an expiry, read on after the last record read; when nothing
  // was read yet, a LATEST shard starts at LATEST again and an AT_SEQUENCE_NUMBER shard at its record again. Otherwise
  // (trimmed, or the last record read is trimmed too) read from the oldest record the shard still holds: everything
  // before it is gone, so nothing is read twice.
  const replacementShardIterator = async error => {
    if (isExpiredError(error) && lastSequenceNumber != null) {
      try {
        return await getShardIterator('AFTER_SEQUENCE_NUMBER', lastSequenceNumber)
      } catch (iteratorError) {
        if (!isTrimmedError(iteratorError)) {
          throw iteratorError
        }
      }
    }
    if (isExpiredError(error) && lastSequenceNumber == null && Shard.ShardIteratorType == 'LATEST') {
      return getShardIterator('LATEST')
    }
    if (isExpiredError(error) && lastSequenceNumber == null && Shard.ShardIteratorType == 'AT_SEQUENCE_NUMBER') {
      try {
        return await getShardIterator('AT_SEQUENCE_NUMBER', Shard.SequenceNumber)
      } catch (iteratorError) {
        if (!isTrimmedError(iteratorError)) {
          throw iteratorError
        }
      }
    }
    return getShardIterator('TRIM_HORIZON')
  }

  // A refused iterator used to end the shard's reads (expired) or throw out of the stream, uncaught, which stopped
  // the service (trimmed). It is replaced instead, a few times at most.
  const getRecordsAt = async ShardIterator => {
    for (let replacements = 0; ; replacements += 1) {
      try {
        const response = await this.retryGetRecords({
          ShardIterator,
          Limit: this.getRecordsLimit,
        })
        this.hook('onGetRecords', Shard, response)
        return response
      } catch (error) {
        if (!(isTrimmedError(error) || isExpiredError(error)) || replacements == SHARD_ITERATOR_REPLACEMENTS) {
          throw error
        }
        console.error(`DynamoStream: ${this.table} shard ${Shard.ShardId}: ${error.code || error.message}; reading on with a new shard iterator`)
        ShardIterator = await replacementShardIterator(error)
      }
    }
  }

  const tagRecords = records => {
    lastSequenceNumber = records[records.length - 1].dynamodb?.SequenceNumber ?? lastSequenceNumber
    return records.map(assign({
      table: always(this.table),
      shardId: always(Shard.ShardId),
    }))
  }

  // the wait after a read: getRecordsInterval, or, on a shard whose reads return nothing, twice the last wait up to
  // idleGetRecordsInterval (equal to getRecordsInterval unless the caller set it higher)
  let interval = this.getRecordsInterval
  const idleInterval = Math.max(this.idleGetRecordsInterval ?? this.getRecordsInterval, this.getRecordsInterval)
  // a shard closed when it was listed gets no new record: its pages are read back to back, at DynamoDB Streams' five
  // reads a second per shard, so a reader catching up through a lineage of closed shards reaches the open one sooner
  const closedShard = Shard.SequenceNumberRange?.EndingSequenceNumber != null
  const waitAfter = records => {
    if (closedShard) {
      return this.sleep(Math.min(this.getRecordsInterval, CLOSED_SHARD_INTERVAL))
    }
    interval = records.Records != null && records.Records.length > 0
      ? this.getRecordsInterval
      : Math.min(Math.max(interval * 2, this.getRecordsInterval), idleInterval)
    return this.sleep(interval)
  }

  const firstIterator = (
    Shard.ShardIteratorType == 'AFTER_SEQUENCE_NUMBER'
    || Shard.ShardIteratorType == 'AT_SEQUENCE_NUMBER'
  ) ? await getShardIterator(Shard.ShardIteratorType, Shard.SequenceNumber)
    : await getShardIterator(Shard.ShardIteratorType)

  let records = await getRecordsAt(firstIterator)

  if (records.Records.length > 0) {
    yield* tagRecords(records.Records)
  }
  // a shard read to its end (closed, every record read) is done at once: its child need not wait a read interval
  if (records.NextShardIterator != null) {
    await waitAfter(records)
  }

  while (!this.closed && records.NextShardIterator != null) {
    records = await getRecordsAt(records.NextShardIterator)
    if (records.Records == null) {
      console.error(`DynamoStream: records.Records undefined: ${records}`)
      break
    } else if (records.Records.length > 0) {
      yield* tagRecords(records.Records)
    }
    if (records.NextShardIterator != null) {
      await waitAfter(records)
    }
  }
}

const SymbolUpdateShards = Symbol('UpdateShards')

// what a shard's reader yields once its reads end (the shard is closed and every record in it was read)
const ShardEnded = function (Shard) {
  this.Shard = Shard
}

// what the timer for a shard's missing child yields
const ListAgain = function () {}

// () => Promise<Array<Shard>>, every shard of every stream of the table: ListStreams, then DescribeStream page by page
DynamoStream.prototype.listShards = async function listShards() {
  const shards = await pipe(this.getStreams(), [
    flatMap(Stream => this.getShards(Stream)),
    transform(Transducer.passthrough, []),
  ])
  this.hook('onShards', shards)
  return shards
}

// Shard => Shard|null, where a shard of the first listing is read from (null: nothing left in it to read); a caller
// may override it on its instance
DynamoStream.prototype.startingShard = function startingShard(Shard) {
  return { ...Shard, ShardIteratorType: this.shardIteratorType }
}

// isOpen(Shard) -> boolean, whether DynamoDB still writes to the shard
const isOpen = Shard => Shard.SequenceNumberRange?.EndingSequenceNumber == null

// mayYield(Shard) -> boolean, whether reading the shard can yield a record: not a closed shard read from LATEST
const mayYield = Shard => !(Shard.ShardIteratorType == 'LATEST' && !isOpen(Shard))

DynamoStream.prototype[Symbol.asyncIterator] = async function* () {
  const readers = new Map() // ShardId -> { Shard, ended: boolean, done: Promise<> }
  const awaitingChild = new Set() // ShardIds of shards whose reads ended with no child known
  let childListings = 0, childTimerPending = false, lastListingAt = 0

  let shards = await this.listShards()

  // a shard's reads, after its parent's when this stream still reads the parent, then ShardEnded
  const startReading = Shard => {
    let markDone = null
    const reader = { Shard, ended: false, done: new Promise(resolve => { markDone = resolve }) }
    readers.set(Shard.ShardId, reader)
    return (async function* readShard() {
      try {
        const parent = readers.get(Shard.ParentShardId)
        if (parent != null && !parent.ended && mayYield(parent.Shard)) {
          await Promise.race([parent.done, this.sleep(PARENT_WAIT)])
        }
        if (!this.closed) {
          yield* this.getRecords(Shard)
        }
      } finally {
        reader.ended = true
        markDone()
      }
      yield new ShardEnded(Shard)
    }).call(this)
  }

  // the safety listing every shardUpdatePeriod, or every 15 s at most while no open shard is known
  const safetyListings = (async function* SafetyListings() {
    while (!this.closed) {
      await this.sleep(shards.some(isOpen)
        ? this.shardUpdatePeriod
        : Math.min(this.shardUpdatePeriod, NO_OPEN_SHARD_PERIOD))
      if (this.closed) {
        return
      }
      yield SymbolUpdateShards
    }
  }).call(this)

  const childTimer = ms => (async function* ChildTimer() {
    await this.sleep(ms)
    if (!this.closed) {
      yield new ListAgain()
    }
  }).call(this)

  const hasChild = (ShardId, list) => list.some(Shard => Shard.ParentShardId == ShardId)

  // a shard the caller says holds nothing left to read (startingShard gave null) counts as read to its end
  const starting = []
  for (const Shard of shards) {
    const start = this.startingShard(Shard)
    if (start == null) {
      readers.set(Shard.ShardId, { Shard, ended: true, done: Promise.resolve() })
    } else {
      starting.push(start)
    }
  }

  let muxAsyncIterator = Mux.race([
    ...starting.map(startReading),
    safetyListings,
  ])

  // lists the shards and starts reading the new ones from TRIM_HORIZON
  const listAndStart = async () => {
    lastListingAt = Date.now()
    const latestShards = await this.listShards()
    const newShards = latestShards
      .filter(Shard => !readers.has(Shard.ShardId))
      .map(Shard => ({ ...Shard, ShardIteratorType: 'TRIM_HORIZON' }))
    shards = latestShards
    for (const ShardId of [...awaitingChild]) {
      if (hasChild(ShardId, latestShards)) {
        awaitingChild.delete(ShardId)
      }
    }
    if (newShards.length > 0) {
      muxAsyncIterator = Mux.race([
        ...newShards.map(startReading),
        muxAsyncIterator,
      ])
    }
  }

  // after a listing: while an ended shard's child is still unknown, list again after 1, 2, 4, 8, 16 s, then leave it
  // to the safety listing
  const followUpChildren = () => {
    if (awaitingChild.size == 0) {
      childListings = 0
      return
    }
    if (childTimerPending) {
      return
    }
    if (childListings >= CHILD_LISTING_RETRIES) {
      console.error(`DynamoStream: ${this.table}: no child found for ${[...awaitingChild].join(', ')}; the next safety listing looks again`)
      awaitingChild.clear()
      childListings = 0
      return
    }
    scheduleChildListing(1000 * (2 ** childListings))
    childListings += 1
  }

  const scheduleChildListing = ms => {
    childTimerPending = true
    muxAsyncIterator = Mux.race([
      childTimer(ms),
      muxAsyncIterator,
    ])
  }

  while (!this.closed) {
    const { value, done } = await muxAsyncIterator.next()
    if (value == SymbolUpdateShards) {
      await listAndStart()
      followUpChildren()
    } else if (value instanceof ShardEnded) {
      if (this.closed || hasChild(value.Shard.ShardId, shards)) {
        continue
      }
      awaitingChild.add(value.Shard.ShardId)
      if (childTimerPending) {
        continue
      }
      if (Date.now() - lastListingAt < 1000) { // shards that end together share one listing
        scheduleChildListing(1000)
        continue
      }
      await listAndStart()
      followUpChildren()
    } else if (value instanceof ListAgain) {
      childTimerPending = false
      await listAndStart()
      followUpChildren()
    } else if (done) {
      await this.sleep(1000)
    } else {
      yield value
    }
  }
}

DynamoStream.DEFAULT_SHARD_UPDATE_PERIOD = DEFAULT_SHARD_UPDATE_PERIOD
DynamoStream.NO_OPEN_SHARD_PERIOD = NO_OPEN_SHARD_PERIOD
DynamoStream.CHILD_LISTING_RETRIES = CHILD_LISTING_RETRIES
DynamoStream.PARENT_WAIT = PARENT_WAIT
DynamoStream.CLOSED_SHARD_INTERVAL = CLOSED_SHARD_INTERVAL

module.exports = DynamoStream
