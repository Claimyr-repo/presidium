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
 * })
 * ```
 *
 * @description
 * https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDBStreams.html
 *
 * DynamoStream -> Stream Headers -> Stream ARNs
 *
 * `eventName` enumeration: `INSERT`, `MODIFY`, `REMOVE`
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
  this.shardUpdatePeriod = options.shardUpdatePeriod ?? 15000
  this.listStreamsLimit = options.listStreamsLimit ?? 100
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
  let lastSequenceNumber = null

  const getShardIterator = (ShardIteratorType, SequenceNumber) => this.retryGetShardIterator({
    ShardId: Shard.ShardId,
    StreamArn: Shard.Stream.StreamArn,
    ShardIteratorType,
    ...SequenceNumber == null ? {} : { SequenceNumber },
  }).then(get('ShardIterator'))

  // An iterator to replace one the stream refused. After an expiry, read on after the last record read; when nothing
  // was read yet, a LATEST shard starts at LATEST again. Otherwise (trimmed, or the last record read is trimmed too)
  // read from the oldest record the shard still holds: everything before it is gone, so nothing is read twice.
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
    return getShardIterator('TRIM_HORIZON')
  }

  // A refused iterator used to end the shard's reads (expired) or throw out of the stream, uncaught, which stopped
  // the service (trimmed). It is replaced instead, a few times at most.
  const getRecordsAt = async ShardIterator => {
    for (let replacements = 0; ; replacements += 1) {
      try {
        return await this.retryGetRecords({
          ShardIterator,
          Limit: this.getRecordsLimit,
        })
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

  let records = await getRecordsAt(await getShardIterator(Shard.ShardIteratorType))

  if (records.Records.length > 0) {
    yield* tagRecords(records.Records)
  }
  await new Promise(resolve => setTimeout(resolve, this.getRecordsInterval))

  while (!this.closed && records.NextShardIterator != null) {
    records = await getRecordsAt(records.NextShardIterator)
    if (records.Records == null) {
      console.error(`DynamoStream: records.Records undefined: ${records}`)
      break
    } else if (records.Records.length > 0) {
      yield* tagRecords(records.Records)
    }
    await new Promise(resolve => setTimeout(resolve, this.getRecordsInterval))
  }
}

const SymbolUpdateShards = Symbol('UpdateShards')

DynamoStream.prototype[Symbol.asyncIterator] = async function* () {
  let shards = await pipe(this.getStreams(), [
    flatMap(Stream => this.getShards(Stream)),
    map(assign({
      ShardIteratorType: always(this.shardIteratorType),
    })),
    transform(Transducer.passthrough, []),
  ])
  let muxAsyncIterator = Mux.race([
    ...shards.map(Shard => this.getRecords(Shard)),
    (async function* UpdateShardsGenerator() {
      while (true) {
        await new Promise(resolve => {
          setTimeout(resolve, this.shardUpdatePeriod)
        })
        yield SymbolUpdateShards
      }
    }).call(this),
  ])

  while (!this.closed) {
    const { value, done } = await muxAsyncIterator.next()
    if (value == SymbolUpdateShards) {
      const latestShards = await pipe(this.getStreams(), [
        flatMap(Stream => this.getShards(Stream)),
        transform(Transducer.passthrough, []),
      ])
      const newShards = pipe(shards, [
        differenceWith(
          (ShardA, ShardB) => ShardA.ShardId == ShardB.ShardId,
          latestShards,
        ),
        map(assign({
          ShardIteratorType: always('TRIM_HORIZON'),
        })),
      ])

      shards = latestShards
      if (newShards.length > 0) {
        muxAsyncIterator = Mux.race([
          ...newShards.map(Shard => this.getRecords(Shard)),
          muxAsyncIterator,
        ])
      }
    } else if (done) {
      await new Promise(resolve => setTimeout(resolve, 1000))
    } else {
      yield value
    }
  }
}

module.exports = DynamoStream
