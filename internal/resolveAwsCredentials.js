const AWS = require('aws-sdk/global')

let chain = null

/**
 * @name resolveAwsCredentials
 *
 * @synopsis
 * ```coffeescript [specscript]
 * resolveAwsCredentials() -> Promise<{
 *   accessKeyId: string,
 *   secretAccessKey: string,
 *   sessionToken?: string,
 * }>
 * ```
 *
 * @description
 * Credentials from the AWS SDK's default provider chain: environment variables, SSO, the shared credentials file,
 * a container role, then the EC2 instance role. For code that signs requests itself (TranscribeStream) and so
 * cannot hand the lookup to an SDK client. Temporary credentials are refreshed when they are close to expiring.
 */
const resolveAwsCredentials = async function () {
  if (chain == null) {
    chain = new AWS.CredentialProviderChain()
  }
  const credentials = await chain.resolvePromise()
  await credentials.getPromise()
  return {
    accessKeyId: credentials.accessKeyId,
    secretAccessKey: credentials.secretAccessKey,
    ...credentials.sessionToken == null ? {} : { sessionToken: credentials.sessionToken },
  }
}

module.exports = resolveAwsCredentials
