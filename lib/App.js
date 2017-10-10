'use strict'

const R = require('ramda')
const _ = require('lodash')
const moment = require('moment')

const {Conv, Random, Time} = require('./helpers')
const disp = require('./Disp')
const storage = require('./Storage')
const Component = require('./Component')
const Address = require('./Address')
const blockchain = require('./Blockchain')
const Block = require('./Block')
const Tx = require('./Tx')
const ifc = require('./Interface')
const p2p = require('./P2P')
const p2x = require('./P2X')
const net = require('./Net')
const synchronizer = require('./Synchronizer')
const packageInfo = require('../package')
const minerChief = require('./MinerChief')
const db = require('./Db')

class App extends Component {

  constructor() {
    super()
    this.module = 'APP'
    this.rpcServer = null
    this.webWallet = null
  }
  
  compileBlocksTemplate({blockHeight, block, branchId}) {
    const blockData = block.getData()
    const lines = []
    
    lines.push('{center}{bold}Block Explorer{/bold}{/center}')
    lines.push('{center}{green-fg}Branch: #' + branchId + ' (master){/green-fg}{/center}')
    lines.push('ID   {bold}' + blockHeight + '{/bold}')
    lines.push('Hash {bold}' + Conv.bufToHex(block.getHash()) + '{/bold}')
    
    lines.push('Prev {bold}' + Conv.bufToHex(blockData.prevBlock) + '{/bold}')
    lines.push('Time {bold}' + moment(blockData.time * 1000 - moment().utcOffset() * 60000).format('YYYY-MM-DD HH:mm:ss') + '{/bold}')
    lines.push('Diff {bold}' + Conv.bufToHex(blockData.diff) + '{/bold}')
    lines.push('Txs  {bold}' + blockData.txCount + '{/bold}')
    lines.push('')
    
    R.forEach((tx) => {
      lines.push('{bold}TX ' + Conv.bufToHex(tx.getHash().toString('hex')) + '{/bold}')
      const txData = tx.getData()
      txData.txIns.each(({txHash, outN}, i) => {
        lines.push('|- IN #' + i + ' ' + Conv.bufToHex(txHash.toString('hex')) + ' #' + outN)
      })
      txData.txOuts.each(({address, value}, i) => {
        lines.push('|- OUT #' + i + ' ' + Address.rawToHash(address) + ' ' + (value / 100000000) + ' XHD')
      })
      lines.push('')
    }, blockData.txList)
    
    return R.join('\n', lines)
  }
  
  run() {
    storage.session.appName = 'Hidecoin Core'
    storage.session.version = packageInfo.version
    storage.logIgnoreModules = {
      P2P: storage.logIgnoreModules && storage.logIgnoreModules.P2P !== undefined ? storage.logIgnoreModules.P2P : true,
      P2X: storage.logIgnoreModules && storage.logIgnoreModules.P2X !== undefined ? storage.logIgnoreModules.P2X : true,
      LCK: storage.logIgnoreModules && storage.logIgnoreModules.LCK !== undefined ? storage.logIgnoreModules.LCK : true
    }
    storage.on('fatalError', (error) => {
      this.log('{red-fg}Fatal error: ' + error + '{/red-fg}')
      disp.terminate(() => {
        ifc.close()
        console.log(error)
      })
    })

    ifc.open()
    ifc.key(['C-c', 'f10'], () => {
      ifc.openWindow('loading')
      ifc.updateWindow('loading', {info: 'Terminating...'})
      disp.terminate()
    })
    ifc.openWindow('loading')
    ifc.updateWindow('loading', {info: 'Synchronizing time...'})
    Time.synchronize((timeOffset) => {
      ifc.updateWindow('loading', {info: 'Connecting to nodes...'})
      p2p.online(storage.config.net && storage.config.net.server && storage.config.net.server.port || 7438, () => {
        ifc.openWindow('app')
        
        setInterval(() => {
          storage.flush()
        }, 60000)
        
        setInterval(() => {
          blockchain.saveCheckpoint()
        }, 600000)
        
        storage.on('synchronize', () => {
          synchronizer.run()
        })
        
        blockchain.on('changed', () => {
          minerChief.updateTask()
        })
        
        this.rpcServer = require('./RpcServer')
        this.rpcServer.on('minerRequestedTask', () => {
          ifc.updateWindow('app', {progressMinerState: true})
        })
        
        this.walletUI = require('./WalletUI')
        require('./Debugger')
        
        let currentBox = 'console'
        let currentBlockHeight = null
        
        ifc.key('f1', () => {
          currentBox = 'console'
          ifc.updateWindow('app', {currentBox})
        })
        
        ifc.key('f2', () => {
          const waiting = setTimeout(() => {
            ifc.notify('Waiting for dispatcher...')
          }, 500)
          blockchain.whenUnlocked((unlock) => {
            clearTimeout(waiting)
            blockchain.getLength((blockchainLength) => {
              if (blockchainLength) {
                currentBox = 'blocks'
                blockchain.getBlockRowInMasterBranchByHeight(blockchainLength - 1, (blockRow, {id}) => {
                  unlock()
                  currentBlockHeight = blockchainLength - 1
                  ifc.updateWindow('app', {currentBox, content: this.compileBlocksTemplate({blockHeight: currentBlockHeight, block: blockchain.rowToBlock(blockRow), branchId: id})})
                }, 1)
              } else {
                unlock()
              }
            }, 1)
          }, 0, 'App[key F2]')
        })
        
        ifc.key('left', () => {
          if (currentBox === 'blocks') {
            const waiting = setTimeout(() => {
              ifc.notify('Waiting for dispatcher...')
            }, 500)
            currentBlockHeight--
            blockchain.getBlockRowInMasterBranchByHeight(currentBlockHeight, (blockRow, {id}) => {
              clearTimeout(waiting)
              if (blockRow) {
                ifc.updateWindow('app', {currentBox: 'blocks', content: this.compileBlocksTemplate({blockHeight: currentBlockHeight, block: blockchain.rowToBlock(blockRow), branchId: id})})
              } else {
                currentBlockHeight++
              }
            })
          }
        })
        
        ifc.key('right', () => {
          if (currentBox === 'blocks') {
            const waiting = setTimeout(() => {
              ifc.notify('Waiting for dispatcher...')
            }, 500)
            currentBlockHeight++
            blockchain.getBlockRowInMasterBranchByHeight(currentBlockHeight, (blockRow, {id}) => {
              clearTimeout(waiting)
              if (blockRow) {
                ifc.updateWindow('app', {currentBox: 'blocks', content: this.compileBlocksTemplate({blockHeight: currentBlockHeight, block: blockchain.rowToBlock(blockRow), branchId: id})})
              } else {
                currentBlockHeight--
              }
            })
          }
        })
        
        ifc.key('f3', () => {
          currentBox = 'miner'
          ifc.updateWindow('app', {currentBox})
        })
        
        ifc.key('f4', () => {
          currentBox = 'wallet'
          ifc.updateWindow('app', {currentBox})
        })
        
        ifc.key('f5', () => {
          currentBox = 'collision'
          ifc.updateWindow('app', {currentBox})
        })
        
        ifc.key('f6', () => {
          ifc.updateWindow('app', {switchHeaderType: true})
        })
        
        ifc.key('f7', () => {
          const currentWindow = ifc.getCurrentWindow()
          if (currentWindow === 'app') {
            ifc.openWindow('wallet')
          } else if (currentWindow === 'wallet') {
            ifc.openWindow('app')
          }
        })
        
        ifc.key('f8', () => {
          const currentWindow = ifc.getCurrentWindow()
          if (currentWindow === 'app') {
            this.log('Nodes:', R.join(', ', R.keys(storage.servers)))
          } else if (currentWindow === 'wallet') {
            this.walletUI.showMenu('options')
          }
        })
        
        ifc.key('f9', () => {
          this.log(storage.session.lockDescriptions)
        })
        
        ifc.key('C-l', () => {
          storage.logIgnoreModules.LCK = !storage.logIgnoreModules.LCK
          storage.flush()
        })
        
        this.log('Synchronizing blockchain...')
        synchronizer.run(() => {
          setTimeout(() => {
            minerChief.updateTask()
          }, 1000)
        })
      })
    })
  }
}

const app = new App
module.exports = app