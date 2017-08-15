'use strict'

const storage = require('./lib/Storage')
storage.init({
  base: __dirname + '/data/',
  path: 'storage.json',
  pathInit: 'init-storage.json'
})

storage.config = require('./config.json')

storage.session.blockchainConfig = {
  basePath: __dirname + '/data/',
  fileName: 'blockchain.db'
}

require('./lib/App').run()