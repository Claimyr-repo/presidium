const AWSKinesis = require('aws-sdk/clients/kinesis')
const placeholderCredentials = require('./internal/placeholderCredentials')

/**
 * @name Kinesis
 *
 * @synopsis
 * ```coffeescript [specscript]
 * Kinesis(options {
 *   accessKeyId?: string,
 *   secretAccessKey?: string,
 *   sessionToken?: string,
 *   region: string,
 *   endpoint?: string,
 * }) -> Kinesis
 * ```
 *
 * @description
 * Kinesis base. https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Kinesis.html
 *
 * Without a key or an endpoint, the AWS SDK resolves credentials itself. See internal/placeholderCredentials.
 */
const Kinesis = function (options) {
  if (this == null || this.constructor != Kinesis) {
    return new Kinesis(options)
  }
  this.client = new AWSKinesis({
    apiVersion: '2013-12-02',
    ...placeholderCredentials(options),
    ...options,
  })
  return this
}

module.exports = Kinesis
