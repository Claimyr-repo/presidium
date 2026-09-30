const Test = require('thunk-test')
const assert = require('assert')
const EventEmitter = require('events')
const fs = require('fs')
const os = require('os')
const path = require('path')
const childProcess = require('child_process')
const placeholderCredentials = require('./internal/placeholderCredentials')
const AwsPresignedUrlV4 = require('./internal/AwsPresignedUrlV4')
const Dynamo = require('./Dynamo')
const Kinesis = require('./Kinesis')
const S3 = require('./S3')
const SecretsManager = require('./SecretsManager')
const EC2 = require('./EC2')
const ECR = require('./ECR')
const AutoScaling = require('./AutoScaling')
const AwsCredentials = require('./AwsCredentials')

// Claimyr's changes to presidium 0.29.13: session tokens pass through, and a client given no key and no endpoint
// lets the AWS SDK resolve credentials (environment, shared file, container or EC2 instance role) instead of
// signing with the 'id'/'secret' placeholders. Nothing here opens a network connection.

const keys = { accessKeyId: 'AKIAEXAMPLEEXAMPLE00', secretAccessKey: 'examplesecretexamplesecretexamplesecret0' }
const temporary = { accessKeyId: 'ASIAEXAMPLEEXAMPLE00', secretAccessKey: 'examplesecretexamplesecretexamplesecret1', sessionToken: 'FwoGZXIvYXdzEXAMPLE+token/value==' }

const withEnv = async function (env, f) {
  const saved = {}
  for (const name in env) {
    saved[name] = process.env[name]
    if (env[name] == null) delete process.env[name]
    else process.env[name] = env[name]
  }
  try {
    return await f()
  } finally {
    for (const name in saved) {
      if (saved[name] == null) delete process.env[name]
      else process.env[name] = saved[name]
    }
  }
}

// no AWS variables and a home directory without .aws/, as on a server that only has an instance role
const emptyHome = fs.mkdtempSync(path.join(os.tmpdir(), 'presidium-credentials-test-'))
const noAwsEnv = {
  AWS_ACCESS_KEY_ID: null,
  AWS_SECRET_ACCESS_KEY: null,
  AWS_SESSION_TOKEN: null,
  AWS_PROFILE: null,
  AWS_SHARED_CREDENTIALS_FILE: null,
  AWS_CONFIG_FILE: null,
  AWS_SDK_LOAD_CONFIG: null,
  HOME: emptyHome,
}

const test = new Test('Claimyr credentials', async function () {
  // placeholderCredentials: only for a local endpoint without keys
  assert.deepEqual(placeholderCredentials({}), {})
  assert.deepEqual(placeholderCredentials({ region: 'us-west-1' }), {})
  assert.deepEqual(placeholderCredentials({ endpoint: 'http://localhost:8000' }), { accessKeyId: 'id', secretAccessKey: 'secret', region: 'x-x-x' })
  assert.deepEqual(placeholderCredentials({ ...keys }), { accessKeyId: 'id', secretAccessKey: 'secret', region: 'x-x-x' })

  // Dynamo and Kinesis: explicit keys win as before, a session token now reaches the SDK
  for (const Client of [Dynamo, Kinesis]) {
    const config = client => (client.connection || client.client).config
    {
      const c = config(new Client({ ...keys, region: 'us-west-1' }))
      assert.equal(c.credentials.accessKeyId, keys.accessKeyId)
      assert.equal(c.credentials.secretAccessKey, keys.secretAccessKey)
      assert.equal(c.region, 'us-west-1')
    }
    {
      const c = config(new Client({ ...temporary, region: 'us-west-1' }))
      assert.equal(c.credentials.accessKeyId, temporary.accessKeyId)
      assert.equal(c.credentials.sessionToken, temporary.sessionToken)
    }
    { // local endpoint without keys: placeholders, unchanged
      const c = config(new Client({ endpoint: 'http://localhost:8000' }))
      assert.equal(c.credentials.accessKeyId, 'id')
      assert.equal(c.credentials.secretAccessKey, 'secret')
      assert.equal(c.region, 'x-x-x')
    }
    // no keys, no endpoint: the SDK resolves credentials itself instead of signing with the placeholders.
    // Checked in a fresh process, since the SDK caches ~/.aws/credentials once it has read it.
    const probe = env => JSON.parse(childProcess.execFileSync(process.execPath, ['-e', `
      const client = new (require('./${Client.name}'))({ region: 'us-west-1' })
      const c = (client.connection || client.client).config
      console.log(JSON.stringify({
        fixed: c.credentials != null,
        placeholder: c.credentials != null && c.credentials.accessKeyId == 'id',
        fromEnvironment: c.credentials != null && c.credentials.accessKeyId == process.env.AWS_ACCESS_KEY_ID,
        withToken: c.credentials != null && c.credentials.sessionToken == process.env.AWS_SESSION_TOKEN,
        chain: c.credentialProvider.providers.length,
        region: c.region,
      }))
    `], { cwd: __dirname, env: { PATH: process.env.PATH, HOME: emptyHome, ...env }, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }))
    { // nothing local, as on a server with only an instance role: left to the provider chain, which ends at that role
      const r = probe({})
      assert.ok(!r.fixed && !r.placeholder, `${Client.name}: no credentials fixed at construction`)
      assert.ok(r.chain > 0, `${Client.name}: the provider chain is there to supply them`)
      assert.equal(r.region, 'us-west-1')
    }
    { // credentials in the environment: the SDK uses them, token included
      const r = probe({ AWS_ACCESS_KEY_ID: temporary.accessKeyId, AWS_SECRET_ACCESS_KEY: temporary.secretAccessKey, AWS_SESSION_TOKEN: temporary.sessionToken })
      assert.ok(r.fromEnvironment && !r.placeholder, `${Client.name}: environment credentials, not the placeholders`)
      assert.ok(r.withToken, `${Client.name}: with their session token`)
    }
  }

  // clients that pick their options: a session token now passes through
  for (const [Client, sdkClient] of [
    [S3, client => client.s3],
    [SecretsManager, client => client.awsSecretsManager],
    [EC2, client => client.awsEc2],
    [ECR, client => client.awsEcr],
    [AutoScaling, client => client.awsAutoScaling],
  ]) {
    const c = sdkClient(new Client({ ...temporary, region: 'us-west-1' })).config
    assert.equal(c.credentials.accessKeyId, temporary.accessKeyId)
    assert.equal(c.credentials.sessionToken, temporary.sessionToken)
  }

  // AwsCredentials: a session token in the environment comes back with the keys
  await withEnv({ AWS_ACCESS_KEY_ID: temporary.accessKeyId, AWS_SECRET_ACCESS_KEY: temporary.secretAccessKey, AWS_SESSION_TOKEN: temporary.sessionToken, AWS_REGION: 'us-west-1' }, async () => {
    assert.deepEqual(await AwsCredentials('default'), { ...temporary, region: 'us-west-1' })
  })
  await withEnv({ AWS_ACCESS_KEY_ID: keys.accessKeyId, AWS_SECRET_ACCESS_KEY: keys.secretAccessKey, AWS_SESSION_TOKEN: null, AWS_REGION: 'us-west-1' }, async () => {
    assert.deepEqual(await AwsCredentials('default'), { ...keys, region: 'us-west-1' })
  })

  // AwsPresignedUrlV4: the session token is signed, in byte order between X-Amz-Expires and X-Amz-SignedHeaders
  {
    const common = {
      region: 'us-west-1',
      method: 'GET',
      endpoint: 'transcribestreaming.us-west-1.amazonaws.com:8443',
      protocol: 'wss',
      canonicalUri: '/stream-transcription-websocket',
      serviceName: 'transcribe',
      payloadHash: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
      queryParams: { 'language-code': 'en-US', 'media-encoding': 'pcm', 'sample-rate': 8000 },
    }
    const plain = new URL(AwsPresignedUrlV4({ ...common, ...keys }))
    assert.equal(plain.searchParams.get('X-Amz-Security-Token'), null)
    const signed = new URL(AwsPresignedUrlV4({ ...common, ...temporary }))
    assert.equal(signed.searchParams.get('X-Amz-Security-Token'), temporary.sessionToken)
    const names = [...signed.searchParams.keys()]
    assert.deepEqual(names.slice(0, 6), ['X-Amz-Algorithm', 'X-Amz-Credential', 'X-Amz-Date', 'X-Amz-Expires', 'X-Amz-Security-Token', 'X-Amz-SignedHeaders'])
    assert.equal(names[names.length - 1], 'X-Amz-Signature')
  }

  // TranscribeStream, with a stand-in socket that records its URL instead of connecting
  {
    const sockets = []
    class FakeWebSocket extends EventEmitter {
      constructor(url) {
        super()
        this.url = new URL(url)
        this.sent = []
        this.closed = false
        sockets.push(this)
      }
      send(bytes) { this.sent.push(bytes) }
      close() { this.closed = true }
    }
    const webSocketPath = require.resolve('./WebSocket')
    const transcribePath = require.resolve('./TranscribeStream')
    const savedWebSocket = require.cache[webSocketPath]
    delete require.cache[transcribePath]
    require.cache[webSocketPath] = { id: webSocketPath, filename: webSocketPath, loaded: true, exports: FakeWebSocket }
    const TranscribeStream = require('./TranscribeStream')
    try {
      const options = { region: 'us-west-1', languageCode: 'en-US', mediaEncoding: 'pcm', sampleRate: 8000 }

      // keys given: connects at once, as before
      const withKeys = new TranscribeStream({ ...options, ...keys })
      assert.equal(sockets.length, 1)
      assert.equal(sockets[0].url.searchParams.get('X-Amz-Security-Token'), null)
      assert.ok(sockets[0].url.searchParams.get('X-Amz-Credential').startsWith(keys.accessKeyId + '/'))
      sockets[0].emit('open')
      await withKeys.ready
      withKeys.sendAudioChunk(Buffer.from([1, 2, 3]))
      assert.equal(sockets[0].sent.length, 1)
      withKeys.close()
      assert.equal(sockets[0].closed, true)

      // temporary keys given: the token is in the URL
      new TranscribeStream({ ...options, ...temporary })
      assert.equal(sockets[1].url.searchParams.get('X-Amz-Security-Token'), temporary.sessionToken)

      // no keys: resolves them from the SDK's chain (here, the environment), then connects
      await withEnv({ ...noAwsEnv, AWS_ACCESS_KEY_ID: temporary.accessKeyId, AWS_SECRET_ACCESS_KEY: temporary.secretAccessKey, AWS_SESSION_TOKEN: temporary.sessionToken }, async () => {
        const fromChain = new TranscribeStream(options)
        assert.equal(sockets.length, 2)
        assert.throws(() => fromChain.sendAudioChunk(Buffer.from([1])), /not connected yet/)
        const ready = fromChain.ready
        while (sockets.length < 3) {
          await new Promise(resolve => setImmediate(resolve))
        }
        assert.ok(sockets[2].url.searchParams.get('X-Amz-Credential').startsWith(temporary.accessKeyId + '/'))
        assert.equal(sockets[2].url.searchParams.get('X-Amz-Security-Token'), temporary.sessionToken)
        sockets[2].emit('open')
        await ready
        fromChain.sendAudioChunk(Buffer.from([1]))
        assert.equal(sockets[2].sent.length, 1)
      })

      // no keys, closed before the credentials arrive: never connects
      await withEnv({ ...noAwsEnv, AWS_ACCESS_KEY_ID: keys.accessKeyId, AWS_SECRET_ACCESS_KEY: keys.secretAccessKey }, async () => {
        const early = new TranscribeStream(options)
        early.close()
        await early.ready
        assert.equal(sockets.length, 3)
      })
    } finally {
      delete require.cache[transcribePath]
      if (savedWebSocket == null) delete require.cache[webSocketPath]
      else require.cache[webSocketPath] = savedWebSocket
    }
  }
}).case()

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
