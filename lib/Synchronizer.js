'use strict'

const fs = require('fs')
const R = require('ramda')

const {Asyncs, Conv, Probable, Time} = require('./helpers')
const disp = require('./Disp')
const storage = require('./Storage')
const Component = require('./Component')
const blockchain = require('./Blockchain')
const net = require('./Net')

const COUNT_PER_REQUEST = 4096
const {RESPONSE_NO_BLOCK, RESPONSE_NO_BLOCK_AFTER} = net.getConstants()

class Synchronizer extends Component {

  constructor() {
    super()
    this.module = 'SNC'
    this.netInfoBlockchainLengths = {}
    this.firstSyncCallback = null
    this.synchronizing = false
    this.db = blockchain.getDb()
    this.initialPrevBlock = blockchain.getInitialPrevBlock()
    this.firstBlockHash = blockchain.getFirstBlockHash()
    
    storage.session.synchronizer = {promiscuous: true, firstReady: false, ready: false, lastBlockAdded: Time.local(), netInfoBlockchainLength: null}
    
    setInterval(() => {
      if (storage.session.synchronizer.lastBlockAdded < Time.local() - 120) {
        this.log('{yellow-fg}Scheduled synchronization...{/yellow-fg}')
        this.sync()
      }
    }, 10000)
    
    setTimeout(() => {
      Time.doNowAndSetInterval(() => {
        net.requestBlockchainLength((err, res) => {
          if (!err) {
            if (res.blockchainLength >= storage.session.blockchain.length) {
              this.netInfoBlockchainLengths[res.address] = res.blockchainLength
              storage.session.synchronizer.netInfoBlockchainLength = Probable.calc(R.values(this.netInfoBlockchainLengths))
            }
          }
        })
      }, 30000)
    }, 5000)
    
    this.sync = () => {
      if (this.synchronizing) {
        this.log('{red-fg}Synchronizer is busy{/red-fg}')
        return
      }
      
      const syncNext = () => {
        setImmediate(() => {
          this.sync()
        })
      }
      
      this.synchronizing = true
      storage.session.synchronizer.ready = false
      
      blockchain.getBranches((branches) => {
        const prevBlocks = []
        const branchLengths = {}
        if (branches.length) {
          R.forEach((branch) => {
            prevBlocks.push({branchId: branch.id, afterHash: branch.lastBlockHash})
            branchLengths[branch.id] = branch.length
          }, branches)
        } else {
          this.log('No branches found')
          prevBlocks.push({branchId: 0, afterHash: blockchain.getInitialPrevBlock()})
        }
        this.log('Partial synchronization {yellow-fg}STARTED{/yellow-fg} (' + branches.length + ' branch(es))')
        
        let branchesSynchronized = 0
        let piecesInQueue = -1
        Asyncs.forEach(prevBlocks, ({branchId, afterHash}, i, nextBranch) => {
          const synchronizeBranch = () => {
            this.log('Synchronizing branch #' + branchId + ' after ' + Conv.bufToHex(afterHash).slice(0, 16) + '...')
            
            let responses = 0
            let noBlockCount = 0
            let noBlockAfterCount = 0
            let added = 0
            let checked = 0
            
            blockchain.whenUnlocked((unlockGlobal) => {
              net.requestBlocksAfter(afterHash, COUNT_PER_REQUEST, (err, res) => {
                responses++
                if (err) {
                  if (err === RESPONSE_NO_BLOCK) {
                    this.log('{yellow-fg}NO_BLOCK{/yellow-fg}')
                    noBlockCount++
                  } else if (err === RESPONSE_NO_BLOCK_AFTER) {
                    this.log('{yellow-fg}NO_BLOCK_AFTER{/yellow-fg}')
                    noBlockAfterCount++
                  }
                } else {
                  piecesInQueue++
                  blockchain.whenUnlocked((unlock) => {
                    res.eachBlock((block, next) => {
                      if (!block) {
                        piecesInQueue--
                        unlock()
                        return
                      }
                      
                      if (!block.wasUnpacked()) {
                        piecesInQueue--
                        unlock()
                        return
                      }
                      
                      const blockHash = block.getHash()
                      const blockData = block.getData()
                      if (!(checked % 20)) {
                        this.logAlias('synchronizing', 'Checking block ' + checked + (piecesInQueue ? '. ' + piecesInQueue + ' piece(s) in queue' : '...'))
                      }
                      checked++
                      blockchain.getBlockIdByHash(blockHash, (blockId) => {
                        if (blockId) { // KNOWN
                          next()
                        } else {
                          const addBlock = (prevBlockHash, prevBlockMeta) => {
                            blockchain.getMasterBranch((masterBranch) => {
                              blockchain.getBranchStructure(masterBranch.id, (branchStructure) => {
                                block.isValidInBranchStructure(branchStructure, (valid, err) => {
                                  if (!valid) {
                                    this.log('{red-fg}Block is NOT valid: ' + err + '{/red-fg}')
                                    next()
                                    return
                                  }
                                  
                                  const blockHeight = prevBlockMeta.height + 1
                                  blockchain.checkForCollision(prevBlockMeta.branchId, blockHeight, (collision) => {
                                    let branchId = prevBlockMeta.branchId
                                    
                                    const addBlockToBranch = () => {
                                      storage.session.synchronizer.lastBlockAdded = Time.local()
                                      blockchain.addBlockToBranch(branchId, blockHeight, block, () => {
                                        branchLengths[branchId]++
                                        added++
                                        onChanged()
                                        next()
                                      }, 4)
                                    }
                                    
                                    if (disp.isSigTerm()) {
                                      unlock()
                                      return
                                    }
                                    
                                    let onChanged
                                    blockchain.change((changed) => {
                                      onChanged = changed
                                      if (collision) {
                                        this.log('Creating new branch due to collision')
                                        blockchain.addBranch(branchId, prevBlockMeta.height + 1, (createdBranchId) => {
                                          branchId = createdBranchId
                                          branchLengths[branchId] = 0
                                          addBlockToBranch()
                                        }, 4)
                                      } else {
                                        addBlockToBranch()
                                      }
                                    }, 2)
                                  }, 2)
                                }, 2)
                              }, 2)
                            }, 2)
                          }
                          
                          if (blockData.prevBlock.equals(this.initialPrevBlock)) {
                            if (!blockHash.equals(this.firstBlockHash)) {
                              next()
                              return
                            }
                            
                            const meta = {branchId: 1, height: -1}
                            addBlock(this.initialPrevBlock, meta)
                          } else {
                            blockchain.getBlockMetaByHash(blockData.prevBlock, (meta) => {
                              if (!meta) {
                                next()
                                return
                              }
                              addBlock(blockData.prevBlock, meta)
                            }, 2)
                          }
                        }
                      }, 2)
                    })
                  }, 1, 'Synchronizer.sync()[Received blocks]')
                }
              }, () => {
                this.log('Waiting for blockchain processes...')
                blockchain.whenUnlocked((unlock) => {
                  const finished = (processNextBranch = true) => {
                    unlock()
                    unlockGlobal()
                    processNextBranch && nextBranch()
                  }
                  
                  if (disp.isSigTerm()) {
                    unlock()
                    unlockGlobal()
                    return
                  }
                  
                  this.logAliasClear('synchronizing')
                  this.log('Branch #' + branchId + ' synchronized')
                  if (responses) {
                    if (!added) {
                      if (noBlockAfterCount) {
                        branchesSynchronized++
                        finished()
                      } else if (noBlockCount) {
                        this.log('Block ' + Conv.bufToHex(afterHash.slice(0, 16)) + ' is isolated')
                        blockchain.getBranchById(branchId, (branch) => {
                          if (branch) {
                            blockchain.change((changed) => {
                              blockchain.removeLastBlockOfBranch(branch, () => {
                                changed()
                                finished()
                              }, 4)
                            }, 2)
                          } else {
                            this.log('Branch #' + branchId + ' has been deleted')
                            finished()
                          }
                        }, 2)
                      } else {
                        finished()
                      }
                    } else {
                      finished()
                    }
                  } else {
                    finished(false)
                    synchronizeBranch()
                  }
                }, 1, 'Synchronizer.sync()[Finished]')
              })
            }, 0, 'Synchronizer.sync()[Global]')
          }
          
          synchronizeBranch()
        }, () => {
          this.log('Partial synchronization {green-fg}FINISHED{/green-fg}')
          this.synchronizing = false
          if (branchesSynchronized === branches.length) {
            this.log('{green-fg}Blockchain synchronized{/green-fg}')
            if (!storage.session.synchronizer.firstReady) {
              storage.session.synchronizer.promiscuous = false
              storage.session.synchronizer.firstReady = true
              this.firstSyncCallback && this.firstSyncCallback()
            }
            storage.session.synchronizer.ready = true
          } else {
            this.log('{yellow-fg}Synchronized ' + branchesSynchronized + ' / ' + branches.length + ' branch(es){/yellow-fg}')
            syncNext()
          }
        })
      })
    }
  }
  
  run(callback) {
    this.firstSyncCallback = callback
    this.sync()
  }
}

const synchronizer = new Synchronizer
module.exports = synchronizer