'use strict'

/* Children can have methods:
*  getInstance(id, callback) (required)
*  // onAddedBlock(int blockId, Block block, callback, currentLockCount)
*  // onRemovedBlocks(int newBlockchainLength, callback, currentLockCount)

** SYSTEM
*  onBlockchainReady(callback, currentLockCount)

** BRANCHES
*  onSwitchedMasterBranch(int fromBranchId, int toBranchId, callback)
*  onRemovedEmptyBranch(int branchId, callback)
*  onAddedBranchFrom(int fromBranchId, int toBranchId, callback)
*  onBranchBecameMaster(int branchId, callback)
*  onAddedBranch(int branchId, callback)

** TXS
*  onFreeTxAdded(Tx tx, callback)
*  onFreeTxDeleted(hash, callback)

** CHECKPOINTS
*  onBeforeSaveCheckpoint(callback)
*  onSaveCheckpoint(string path, callback)
*  onLoadCheckpoint(string path, callback)
*/

const storage = require('./Storage')
const Component = require('./Component')

module.exports = class BlockchainPlugin extends Component {

  constructor() {
    super()
    this.module = 'PLG'
  }
  
  registerIfNeeded(className, id, callback) {
    for (const i in storage.plugins.blockchain) {
      const plugin = storage.plugins.blockchain[i]
      if (plugin.className === className && plugin.id === id) {
        callback && callback(false)
        return
      }
    }
    storage.plugins.blockchain.push({className, id})
    storage.flush(() => {
      callback && callback(true)
    })
  }
  
  static unregisterIfNeeded(className, id, callback) {
    for (const i in storage.plugins.blockchain) {
      const plugin = storage.plugins.blockchain[i]
      if (plugin.className === className && plugin.id === id) {
        storage.plugins.blockchain.splice(i, 1)
        storage.flush(() => {
          callback && callback(true)
        })
        return
      }
    }
    callback && callback(false)
  }
}