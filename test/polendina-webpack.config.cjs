const NodePolyfillPlugin = require('node-polyfill-webpack-plugin')

module.exports = {
  resolve: {
    fallback: {
      assert: require.resolve('assert-polyfill')
    }
  },
  plugins: [new NodePolyfillPlugin()]
}
