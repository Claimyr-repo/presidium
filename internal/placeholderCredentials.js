/**
 * @name placeholderCredentials
 *
 * @synopsis
 * ```coffeescript [specscript]
 * placeholderCredentials(options {
 *   accessKeyId?: string,
 *   secretAccessKey?: string,
 *   endpoint?: string,
 * }) -> {
 *   accessKeyId: 'id',
 *   secretAccessKey: 'secret',
 *   region: 'x-x-x',
 * }|{}
 * ```
 *
 * @description
 * The dummy credentials a client starts from, spread before the caller's options so any key the caller passes
 * still wins. They exist for local endpoints (DynamoDB Local, kinesalite) that accept any key.
 *
 * With no key and no endpoint there is nothing to stand in for, so this returns `{}` and the AWS SDK resolves
 * real credentials itself: environment variables, the shared credentials file, a container role or the EC2
 * instance role. Before, those callers signed with 'id'/'secret' and every request failed.
 */
const placeholderCredentials = function (options) {
  if (
    options.accessKeyId == null
    && options.secretAccessKey == null
    && options.endpoint == null
  ) {
    return {}
  }
  return {
    accessKeyId: 'id',
    secretAccessKey: 'secret',
    region: 'x-x-x',
  }
}

module.exports = placeholderCredentials
