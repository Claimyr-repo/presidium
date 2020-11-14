const fs = require('fs')
const assert = require('assert')
const Test = require('thunk-test')
const Redis = require('./Redis')
const RedisString = require('./RedisString')

module.exports = Test('RedisString', RedisString)
  .before(async function () {
    try {
      await fs.promises.rm(`${__dirname}/tmp.dump`)
    } catch {}
    this.redis = new Redis('redis://localhost:6379/')
  })
  .beforeEach(async function () {
    await this.redis.connection.flushdb()
  })
  .case('redis://localhost:6379', 'test-string', async function (testString) {
    this.testString = testString
    await testString.set('hey', 'XX')
    assert(await testString.get() == null)
    await testString.set('hey', 'NX')
    assert(await testString.get() == 'hey')
    await testString.set(0)
    assert(await testString.incr() == 1)
    assert(await testString.decr() == 0)
    assert(await testString.incrby(3) == 3)
    assert(await testString.decrby(0) == 3)
    assert(await testString.decrby(-2) == 5)
    assert(await testString.strlen() == 1)
    assert(typeof await testString.strlen() == 'number')
    assert(await testString.getset('hey') == 5)
    assert(await testString.get() == 'hey')
    assert(await testString.get(0) == 'h')
    assert(await testString.get(1) == 'e')
    assert(await testString.get(2) == 'y')
    assert(await testString.get(-1) == 'y')
    assert(await testString.get(-2) == 'e')
    assert(await testString.get(-3) == 'h')
    assert(await testString.get(3) === '')
    await testString.set('immediate-expire', 'PX', 1)
    await new Promise(resolve => setTimeout(resolve, 10))
    assert(await testString.get() == null)
    assert.deepEqual(await this.redis.scan(0), ['0', []])
    await testString.set('hey')
    assert.deepEqual(await this.redis.scan(0), ['0', ['test-string']])
    assert(await this.redis.ttl('test-string') == -1)
    assert(await this.redis.pttl('test-string') == -1)
    assert(await this.redis.expire('test-string', 1e6) == 1)
    assert(await this.redis.ttl('test-string') > 0)
    assert(await this.redis.pttl('test-string') > 0)
    assert(await this.redis.expireat('test-string', Date.now() + 1e6) == 1)
    assert(await this.redis.ttl('test-string') > 0)
    assert(await this.redis.pttl('test-string') > 0)
    assert(await this.redis.type('test-string') == 'string')
    assert(await this.redis.del('test-string') == 1)
    await testString.set('hey')
    assert(await this.redis.unlink('test-string') == 1)
  })
  .after(async function () {
    await this.redis.connection.flushdb()
    this.redis.connection.disconnect()
    this.testString.connection.disconnect()
  })
