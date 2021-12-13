/**
 * @name AmzSignedHeaders
 *
 * @synopsis
 * ```coffeescript [specscript]
 * AmzSignedHeaders(headers Object) -> amzSignedHeaders string
 * ```
 */
const AmzSignedHeaders = function (headers) {
  return Object.keys(headers).map(key => key.toLowerCase()).sort()
}

module.exports = AmzSignedHeaders
