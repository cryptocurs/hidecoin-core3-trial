'use strict'

/*
Notes:
1. There are may be transactions with the same hashes in one branch of blockchain
*/

const R = require('ramda')

const {Asyncs, Conv, Files, Sorted, Time} = require('./helpers')
const disp = require('./Disp')
const storage = require('./Storage')
const Db = require('./Db')
const Component = require('./Component')
const Address = require('./Address')
const BufferArray = require('./BufferArray')
const ScalableBufferArray = require('./ScalableBufferArray')
const SteppedBuffer = require('./SteppedBuffer')

const BASE_PATH = __dirname + '/../data/'
const PATH_CHECKPOINTS = BASE_PATH + 'checkpoints/'

const INITIAL_PREV_BLOCK = Buffer.from('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF', 'hex')
const FIRST_BLOCK_HASH = Buffer.from('00091c70a5766a655134e1a93cee11887515ad34c4f9d4b4287c7994d821cc33', 'hex')

class Blockchain extends Component {

  constructor() {
    super()
    this.module = 'BLK'
    this.length = 0
    
    this.db = Db(storage.session.blockchainConfig)
    
    this.dbCreateTables = () => {
      return new Promise((resolve) => {
        /*
        Notes:
          There may be same transactions in different branch structures
        Table `branches`
          parentId - parent branch
          isMaster - branch is last in master branch structure
          length - height of last block in branch structure + 1
          blockId - id of first block in branch structure
          lastBlockHash - hash of last block in branch structure
        Table `outs`
          inMasterBranch - block with tx with this out belongs to master branch structure
          spentAt - spent at block (id)
        Table `spends`
          txHash, outN - tx data
          spentAt - spent at block (id)
        */
        this.db.query("CREATE TABLE IF NOT EXISTS branches (id INTEGER PRIMARY KEY, parentId INTEGER, isMaster INTEGER, length INTEGER, blockId INTEGER, lastBlockHash BLOB)")
          .then(() => this.db.query("CREATE TABLE IF NOT EXISTS blocks (id INTEGER PRIMARY KEY, branchId INTEGER, height INTEGER, prevBlock BLOB, time INTEGER, hash BLOB, data BLOB)"))
          .then(() => this.db.query("CREATE TABLE IF NOT EXISTS txs (id INTEGER PRIMARY KEY, blockId INTEGER, hash BLOB)"))
          .then(() => this.db.query("CREATE TABLE IF NOT EXISTS outs (id INTEGER PRIMARY KEY, blockId INTEGER, blockHeight INTEGER, txHash BLOB, outN INTEGER, address BLOB, amount INTEGER, inMasterBranch INTEGER, spentAt INTEGER)"))
          .then(() => this.db.query("CREATE TABLE IF NOT EXISTS spends (id INTEGER PRIMARY KEY, txHash BLOB, outN INTEGER, spentAt INTEGER)"))
          
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS branchId ON blocks (branchId)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS height ON blocks (height)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS time ON blocks (time)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS hash ON blocks (hash)"))
          
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS blockId ON txs (blockId)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS hash ON txs (hash)"))
          
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS blockId ON outs (blockId)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS blockHeight ON outs (blockHeight)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS txHash ON outs (txHash)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS address ON outs (address)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS inMasterBranch ON outs (inMasterBranch)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS spentAt ON outs (spentAt)"))
          
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS txHash ON spends (txHash)"))
          .then(() => this.db.query("CREATE INDEX IF NOT EXISTS spentAt ON spends (spentAt)"))
          
          .then(() => {
            this.db.each("SELECT COUNT(id) cnt FROM branches", ({cnt}) => {
              if (cnt) {
                resolve()
              } else {
                this.db.query("INSERT INTO branches (parentId, isMaster, length, lastBlockHash) VALUES (?, ?, ?, ?)", [0, 1, 0, INITIAL_PREV_BLOCK])
                  .then(() => resolve())
                  .catch((err) => storage.emit('fatalError', err))
              }
            })
          })
          .catch((err) => storage.emit('fatalError', err))
      })
    }
    
    this.dbClear = () => {
      return new Promise((resolve) => {
        this.db.query("DROP TABLE branches")
          .then(() => this.db.query("DROP TABLE blocks"))
          .then(() => this.db.query("DROP TABLE txs"))
          .then(() => this.db.query("DROP TABLE outs"))
          .then(() => this.createTables())
          .then(() => resolve())
          .catch((err) => storage.emit('fatalError', err))
      })
    }
    
    this.updateLength = () => {
      return new Promise((resolve) => {
        this.db.each("SELECT MAX(height) maxHeight FROM blocks", (res) => {
          this.length = res.maxHeight !== null ? res.maxHeight + 1 : 0
          storage.session.blockchain.length = this.length
          resolve()
        })
      })
      .catch((err) => storage.emit('fatalError', err))
    }
    
    this.eachPlugin = (itemCallback, returnCallback) => {
      Asyncs.forEach(storage.plugins.blockchain, ({className, id}, i, next) => {
        const classDef = require('../plugins/' + className)
        classDef.getInstance(id, (instance) => {
          if (instance) {
            itemCallback(instance, next)
          } else {
            next()
          }
        })
      }, returnCallback)
    }
    
    this.logBranches = (...data) => {
      this.log(...data)
      this.logBy('COL', ...data)
    }
    
    Files.needDir(PATH_CHECKPOINTS)
    
    this.lock()
    this.dbCreateTables()
      .then(() => this.updateLength())
      .then(() => {
        this.eachPlugin((instance, next) => {
          if (instance.onBlockchainReady) {
            instance.onBlockchainReady(next, 1)
          } else {
            next()
          }
        }, () => this.unlock())
      })
    
    storage.session.blockchain = {length: 0}
    if (!storage.plugins) {
      storage.plugins = {}
    }
    if (!storage.plugins.blockchain) {
      storage.plugins.blockchain = []
    }
    
    storage.on('getLockQueueLength', (callback) => {
      callback(this.getLockQueueLength())
    })
    
    this.freeTxs = ScalableBufferArray({
      step: 65536,
      fields: {
        hash: {type: 'buffer', size: 32},
        data: {type: 'buffer'},
        added: {type: 'number', size: 8}
      }
    })
    
    this.onFreeTxAdded = (tx) => {
      this.eachPlugin((instance, next) => {
        if (instance.onFreeTxAdded) {
          instance.onFreeTxAdded(tx, next)
        } else {
          next()
        }
      })
    }
    
    this.onFreeTxDeleted = (hash) => {
      this.eachPlugin((instance, next) => {
        if (instance.onFreeTxDeleted) {
          instance.onFreeTxDeleted(hash, next)
        } else {
          next()
        }
      })
    }
    
    this.switchMasterBranchIfNeeded = (allowableLockCount = 0) => {
      return new Promise((resolve) => {
        this.whenUnlocked((unlock) => {
          const ret = () => {
            disp.unlockTerm()
            unlock()
            resolve()
          }
          
          disp.lockTerm()
          this.db.getRow("SELECT * FROM branches ORDER BY length DESC, id ASC LIMIT 1")
            .then((branch) => {
              if (branch.isMaster) {
                ret()
                return
              }
              
              this.db.getRow("SELECT * FROM branches WHERE isMaster=1 LIMIT 1")
                .then((masterBranch) => {
                  this.db.query("UPDATE branches SET isMaster=0 WHERE id=?", [masterBranch.id])
                    .then(() => this.db.query("UPDATE branches SET isMaster=1 WHERE id=?", [branch.id]))
                    .then(() => {
                      const oldBlockIds = []
                      this.getBranchStructure(branch.id, (branchStructure) => {
                        this.rEachInBranch(masterBranch, (blockRow, next) => {
                          this.isBlockInBranchStructure(blockRow, branchStructure, (isIn) => {
                            if (isIn) {
                              next(true)
                            } else {
                              oldBlockIds.push(blockRow.id)
                              next()
                            }
                          }, allowableLockCount + 2)
                        }, () => {
                          const newBlockIds = []
                          this.getBranchStructure(masterBranch.id, (branchStructure) => {
                            this.rEachInBranch(branch, (blockRow, next) => {
                              this.isBlockInBranchStructure(blockRow, branchStructure, (isIn) => {
                                if (isIn) {
                                  next(true)
                                } else {
                                  newBlockIds.push(blockRow.id)
                                  next()
                                }
                              }, allowableLockCount + 2)
                            }, () => {
                              const oldBlocksPlaceholders = R.join(',', R.repeat('?', oldBlockIds.length))
                              this.db.query("UPDATE outs SET inMasterBranch=0, spentAt=0 WHERE blockId IN (" + oldBlocksPlaceholders + ")", oldBlockIds)
                                .then(() => this.db.query("UPDATE outs SET spentAt=0 WHERE spentAt IN (" + oldBlocksPlaceholders + ")", oldBlockIds))
                                .then(() => {
                                  const newBlocksPlaceholders = R.join(',', R.repeat('?', newBlockIds.length))
                                  this.db.query("UPDATE outs SET inMasterBranch=1 WHERE blockId IN (" + newBlocksPlaceholders + ")", newBlockIds)
                                    .then(() => this.db.getAll("SELECT * FROM spends WHERE spentAt IN (" + newBlocksPlaceholders + ")", newBlockIds))
                                    .then((spends) => {
                                      Asyncs.forEach(spends, ({txHash, outN, spentAt}, i, next) => {
                                        this.restoreBlockIdFromTxHashSpentAt(txHash, spentAt, (blockId) => {
                                          this.db.query("UPDATE outs SET spentAt=? WHERE blockId=? AND txHash=? AND outN=?", [spentAt, blockId, txHash, outN])
                                            .then(() => next())
                                            .catch((err) => storage.emit('fatalError', err))
                                        }, allowableLockCount + 1)
                                      }, () => {
                                        this.logBranches('{yellow-fg}Switched master branch from #' + masterBranch.id + ' to #' + branch.id + '{/yellow-fg}')
                                        this.eachPlugin((instance, next) => {
                                          if (instance.onSwitchedMasterBranch) {
                                            instance.onSwitchedMasterBranch(masterBranch.id, branch.id, next)
                                          } else {
                                            next()
                                          }
                                        }, () => ret())
                                      })
                                    })
                                    .catch((err) => storage.emit('fatalError', err))
                                })
                                .catch((err) => storage.emit('fatalError', err))
                            }, null, allowableLockCount + 1)
                          }, allowableLockCount + 1)
                        }, null, allowableLockCount + 1)
                      }, allowableLockCount + 1)
                    })
                    .catch((err) => storage.emit('fatalError', err))
                })
                .catch((err) => storage.emit('fatalError', err))
            })
            .catch((err) => storage.emit('fatalError', err))
        }, allowableLockCount, 'Blockchain.switchMasterBranchIfNeeded()')
      })
      .catch((err) => storage.emit('fatalError', err))
    }
    
    this.removeBranchIfEmpty = (branch, allowableLockCount = 0) => {
      return new Promise((resolve) => {
        this.whenUnlocked((unlock) => {
          const ret = (res) => {
            disp.unlockTerm()
            unlock()
            resolve(res)
          }
          
          disp.lockTerm()
          this.db.getRow("SELECT COUNT(id) cnt FROM blocks WHERE branchId=?", [branch.id])
            .then(({cnt}) => {
              if (!cnt) {
                this.db.query("DELETE FROM branches WHERE id=?", [branch.id])
                  .then(() => {
                    this.logBranches('{yellow-fg}Removed branch #' + branch.id + '{/yellow-fg}')
                    this.eachPlugin((instance, next) => {
                      if (instance.onRemovedEmptyBranch) {
                        instance.onRemovedEmptyBranch(branch.id, next)
                      } else {
                        next()
                      }
                    }, () => ret(true))
                  })
                  .catch((err) => storage.emit('fatalError', err))
              } else {
                this.db.getRow("SELECT * FROM blocks WHERE prevBlock=? AND branchId>? ORDER BY branchId ASC LIMIT 1", [branch.lastBlockHash, branch.id])
                  .then((neighbor) => {
                    if (!neighbor) {
                      ret(false)
                      return
                    }
                    
                    const fromBranchId = neighbor.branchId
                    const toBranchId = branch.id
                    this.getBranchById(fromBranchId, (fromBranch) => {
                      this.updateBranch(toBranchId, fromBranch.length, fromBranch.lastBlockHash, () => {
                        this.db.query("DELETE FROM branches WHERE id=?", [fromBranchId])
                          .then(() => this.db.query("UPDATE branches SET parentId=? WHERE parentId=?", [toBranchId, fromBranchId]))
                          .then(() => this.db.query("UPDATE blocks SET branchId=? WHERE branchId=?", [toBranchId, fromBranchId]))
                          .then(() => {
                            this.logBranches('{yellow-fg}Added branch #' + fromBranchId + ' to branch #' + toBranchId + '{/yellow-fg}')
                            this.eachPlugin((instance, next) => {
                              if (instance.onAddedBranchFrom) {
                                instance.onAddedBranchFrom(fromBranchId, toBranchId, next)
                              } else {
                                next()
                              }
                            }, () => {
                              if (fromBranch.isMaster) {
                                this.db.query("UPDATE branches SET isMaster=? WHERE id=?", [1, toBranchId])
                                  .then(() => {
                                    this.logBranches('{yellow-fg}Branch #' + toBranchId + ' became master{/yellow-fg}')
                                    this.eachPlugin((instance, next) => {
                                      if (instance.onBranchBecameMaster) {
                                        instance.onBranchBecameMaster(toBranchId, next)
                                      } else {
                                        next()
                                      }
                                    }, () => ret(true))
                                  })
                                  .catch((err) => storage.emit('fatalError', err))
                              } else {
                                ret(true)
                              }
                            })
                          })
                          .catch((err) => storage.emit('fatalError', err))
                      }, allowableLockCount + 1)
                    }, allowableLockCount + 1)
                  })
                  .catch((err) => storage.emit('fatalError', err))
              }
            })
            .catch((err) => storage.emit('fatalError', err))
        }, allowableLockCount, 'Blockchain.removeBranchIfEmpty()')
      })
      .catch((err) => storage.emit('fatalError', err))
    }
  }
  
  change(callback, allowableLockCount = 0) {
    this.whenUnlocked((unlockGlobal) => {
      this.db.begin().then(() => {
        this.whenUnlocked((unlock) => {
          callback(() => {
            this.db.commit().then(() => {
              unlock()
              unlockGlobal()
              this.emit('changed')
            })
            .catch((err) => storage.emit('fatalError', err))
          })
        }, allowableLockCount + 1, 'Blockchain.change():begin')
      })
      .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.change()')
  }
  
  addBranch(parentId, length, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.db.query("INSERT INTO branches (parentId, isMaster, length) VALUES (?, ?, ?)", [parentId, 0, length])
        .then((branchId) => {
          this.logBranches('{yellow-fg}Created new branch #' + branchId + '{/yellow-fg}')
          this.eachPlugin((instance, next) => {
            if (instance.onAddedBranch) {
              instance.onAddedBranch(branchId, next)
            } else {
              next()
            }
          }, () => {
            unlock()
            callback(branchId)
          })
        })
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.addBranch()')
  }
  
  addBlockToBranch(branchId, height, block, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const blockHash = block.getHash()
      const blockData = block.getData()
      disp.lockTerm()
      this.logBy('LCK', '<TERM> locked by Blockchain.addBlockToBranch()')
      this.db.query("INSERT INTO blocks (branchId, height, prevBlock, time, hash, data) VALUES (?, ?, ?, ?, ?, ?)", [branchId, height, blockData.prevBlock, blockData.time, blockHash, block.getRawData()])
        .then((blockId) => new Promise((resolve) => {
          this.isBranchMaster(branchId, (isBranchMaster) => {
            const queryUpdateOutsValues = []
            Asyncs.forEach(blockData.txList, (tx, i, nextTx) => {
              const txHash = tx.getHash()
              const txData = tx.getData()
              this.db.query("INSERT INTO txs (blockId, hash) VALUES (?, ?)", [blockId, txHash])
                .then(() => {
                  txData.txIns.eachAsync((txIn, i, raw, next) => {
                    this.db.query("INSERT INTO spends (txHash, outN, spentAt) VALUES (?, ?, ?)", [txIn.txHash, txIn.outN, blockId]).then(() => {
                      if (isBranchMaster) {
                        queryUpdateOutsValues.push(txIn.txHash, txIn.outN)
                      }
                      next()
                    })
                    .catch((err) => storage.emit('fatalError', err))
                  }, () => {
                    txData.txOuts.eachAsync(({address, value}, outN, raw, next) => {
                      this.db.query("INSERT INTO outs (blockId, blockHeight, txHash, outN, address, amount, inMasterBranch, spentAt) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [blockId, height, txHash, outN, address, value, isBranchMaster ? 1 : 0, 0])
                        .then(next)
                        .catch((err) => storage.emit('fatalError', err))
                    }, () => {
                      nextTx()
                    })
                  })
                })
            }, () => new Promise((resolve) => {
              if (isBranchMaster && queryUpdateOutsValues.length) {
                const queryUpdateOutsValuesItems = R.splitEvery(996, queryUpdateOutsValues)
                Asyncs.forEach(queryUpdateOutsValuesItems, (queryUpdateOutsValuesItem, i, next) => {
                  this.db.query("UPDATE outs SET spentAt=? WHERE (" + R.join(' OR ', R.repeat("txHash=? AND outN=?", queryUpdateOutsValuesItem.length >> 1)) + ") AND inMasterBranch=?", [blockId, ...queryUpdateOutsValuesItem, 1])
                    .then(next)
                    .catch((err) => storage.emit('fatalError', err))
                }, () => {
                  resolve()
                })
              } else {
                resolve()
              }
            })
              .then(() => this.db.query("UPDATE branches SET length=length+1, lastBlockHash=? WHERE id=?", [blockHash, branchId]))
              .then(() => this.db.query("UPDATE branches SET blockId=? WHERE id=? AND blockId IS NULL", [blockId, branchId]))
              .then(() => this.switchMasterBranchIfNeeded(allowableLockCount + 1))
              .then(() => resolve())
              .catch((err) => storage.emit('fatalError', err))
            )
          }, allowableLockCount + 1)
        }))
        .then(() => this.updateLength())
        .then(() => {
          this.logBy('LCK', '<TERM> unlocked by Blockchain.addBlockToBranch()')
          disp.unlockTerm()
          unlock()
          callback()
        })
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.addBlockToBranch()')
  }
  
  updateBranch(branchId, length, lastBlockHash, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.db.query("UPDATE branches SET length=?, lastBlockHash=? WHERE id=?", [length, lastBlockHash, branchId])
        .then(() => {
          unlock()
          callback()
        })
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.updateBranch()')
  }
  
  removeLastBlockOfBranch(branch, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      disp.lockTerm()
      let branchHasBlocks = true
      this.getBlockMetaByHash(branch.lastBlockHash, (blockMeta) => {
        const lastBlockId = blockMeta.id
        this.db.query("DELETE FROM blocks WHERE id=?", [lastBlockId])
          .then(() => this.db.query("DELETE FROM txs WHERE blockId=?", [lastBlockId]))
          .then(() => this.db.query("DELETE FROM outs WHERE blockId=?", [lastBlockId]))
          .then(() => this.db.query("UPDATE outs SET spentAt=0 WHERE spentAt=?", [lastBlockId]))
          .then(() => this.db.query("DELETE FROM spends WHERE spentAt=?", [lastBlockId]))
          .then(() => {
            branch.length--
            branch.lastBlockHash = blockMeta.prevBlock
            this.updateBranch(branch.id, branch.length, branch.lastBlockHash, () => {
              this.removeBranchIfEmpty(branch, allowableLockCount + 1)
                .then((removed) => {
                  branchHasBlocks = !removed
                  return this.switchMasterBranchIfNeeded(allowableLockCount + 1)
                })
                .then(() => this.updateLength())
                .then(() => {
                  disp.unlockTerm()
                  unlock()
                  callback(branchHasBlocks)
                })
                .catch((err) => storage.emit('fatalError', err))
            }, allowableLockCount + 1)
          })
          .catch((err) => storage.emit('fatalError', err))
      }, allowableLockCount + 1)
    }, allowableLockCount, 'Blockchain.removeLastBlockOfBranch()')
  }
  
  removeBranch(branch, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const removeNextBlock = () => {
        this.removeLastBlockOfBranch(branch, (branchHasBlocks) => {
          if (branchHasBlocks) {
            setImmediate(removeNextBlock)
          } else {
            unlock()
            callback()
          }
        }, allowableLockCount + 1)
      }
      
      removeNextBlock()
    }, allowableLockCount, 'Blockchain.removeBranch()')
  }
  
  findOutdatedBranch(callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.db.getRow("SELECT * FROM branches WHERE length<?", [this.length - 10000])
        .then((branch) => {
          unlock()
          callback(branch)
        })
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.findOutdatedBranch()')
  }
  
  removeOutdatedBranches(callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const removeNextBranch = () => {
        this.findOutdatedBranch((branch) => {
          if (!branch) {
            unlock()
            callback()
            return
          }
          
          this.removeBranch(branch, () => {
            setImmediate(removeNextBranch)
          }, allowableLockCount + 1)
        }, allowableLockCount + 1)
      }
      
      removeNextBranch()
    }, allowableLockCount, 'Blockchain.removeOutdatedBranches()')
  }
  
  rowToBlock(row) {
    const Block = require('./Block')
    return Block.fromRaw(row.hash, row.data)
  }
  
  rEachInBranch(branch, itemCallback, returnCallback, returnDefault = null, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      let nextHash = branch.lastBlockHash
      const ret = (res) => {
        unlock()
        returnCallback && returnCallback(res)
      }
      
      const readNext = () => {
        this.getBlockRowByHash(nextHash, (blockRow) => {
          itemCallback(blockRow, (res) => {
            if (res !== undefined) {
              ret(res)
            } else {
              if (blockRow.height) {
                nextHash = blockRow.prevBlock
                setImmediate(() => {
                  readNext()
                })
              } else {
                ret(returnDefault)
              }
            }
          })
        }, allowableLockCount + 1)
      }
        
      readNext()
    }, allowableLockCount, 'Blockchain.rEachInBranch()')
  }
  
  // not work with asynchronous functions
  // in all branches, not compatible with Core II
  eachAfter(hash, count, itemCallback, returnCallback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        returnCallback && returnCallback(res)
      }
      
      const getBlocks = () => {
        let isFinished = false
        this.db.each("SELECT * FROM blocks WHERE id>? LIMIT " + count, [id], (row) => {
          !isFinished && itemCallback(row, (finished) => {
            if (finished) {
              isFinished = true
            }
          })
        }, (rowCount) => {
          ret(rowCount)
        })
      }
      
      let id
      if (hash.equals(INITIAL_PREV_BLOCK)) {
        id = 0
        getBlocks()
      } else {
        this.getBlockIdByHash(hash, (blockId) => {
          if (!blockId) {
            ret(-1)
            return
          }
          
          id = blockId
          getBlocks()
        }, allowableLockCount + 1)
      }
    }, allowableLockCount, 'Blockchain.eachAfter()')
  }
  
  // not work with asynchronous functions
  // in all branches, not compatible with Core II
  // forced
  eachAfterForced(hash, count, itemCallback, returnCallback) {
    const ret = (res) => {
      returnCallback && returnCallback(res)
    }
    
    const getBlocks = () => {
      let isFinished = false
      this.db.each("SELECT * FROM blocks WHERE id>? LIMIT " + count, [id], (row) => {
        !isFinished && itemCallback(row, (finished) => {
          if (finished) {
            isFinished = true
          }
        })
      }, (rowCount) => {
        ret(rowCount)
      })
    }
    
    let id
    if (hash.equals(INITIAL_PREV_BLOCK)) {
      id = 0
      getBlocks()
    } else {
      this.getBlockIdByHashForced(hash, (blockId) => {
        if (!blockId) {
          ret(-1)
          return
        }
        
        id = blockId
        getBlocks()
      })
    }
  }
  
  // in master branch, compatible with Core II
  eachInMasterBranchAfter(hash, count, itemCallback, returnCallback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        returnCallback && returnCallback(res)
      }
      
      blockchain.getMasterBranch((masterBranch) => {
        blockchain.getBranchStructure(masterBranch.id, (branchStructure) => {
          const getBlocks = () => {
            const nextBlock = () => {
              this.getBlockRowInBranchStructureByHeight(branchStructure, ++height, (blockRow) => {
                if (!blockRow) {
                  ret(added)
                  return
                }
                
                itemCallback(blockRow, (finished) => {
                  if (++added === count || finished) {
                    ret(added)
                  } else {
                    nextBlock()
                  }
                })
              }, allowableLockCount + 1)
            }
            
            let added = 0
            nextBlock()
          }
          
          let height
          if (hash.equals(INITIAL_PREV_BLOCK)) {
            height = -1
            getBlocks()
          } else {
            this.getBlockMetaByHash(hash, (blockMeta) => {
              if (!blockMeta) {
                ret(-1)
                return
              }
              
              this.isBlockInBranchStructure(blockMeta, branchStructure, (isIn) => {
                if (!isIn) {
                  ret(-1)
                  return
                }
                
                height = blockMeta.height
                getBlocks()
              }, allowableLockCount + 1)
            }, allowableLockCount + 1)
          }
        }, allowableLockCount + 1)
      }, allowableLockCount + 1)
    }, allowableLockCount, 'Blockchain.eachInMasterBranchAfter()')
  }
  
  isBranchMaster(branchId, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.db.getRow("SELECT id FROM branches WHERE id=? AND isMaster=1", branchId)
        .then((row) => {
          unlock()
          callback(!!row)
        })
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.isBranchMaster()')
  }
  
  getMasterBranch(callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      this.db.each("SELECT * FROM branches WHERE isMaster=1", (row) => {
        ret(row)
      }, (rowCount) => {
        if (!rowCount) {
          storage.emit('fatalError', 'No master branch')
        }
      })
    }, allowableLockCount, 'Blockchain.getMasterBranch()')
  }
  
  getBranchById(id, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      this.db.each("SELECT * FROM branches WHERE id=?", [id], (row) => {
        ret(row)
      }, (rowCount) => {
        if (!rowCount) {
          ret(null)
        }
      })
    }, allowableLockCount, 'Blockchain.getBranchById()')
  }
  
  getBranchCount(callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.db.getRow("SELECT COUNT(id) cnt FROM branches")
        .then(({cnt}) => {
          unlock()
          callback(cnt)
        })
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.getBranchCount()')
  }
  
  getBranches(callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const branches = []
      this.db.each("SELECT * FROM branches", (row) => {
        branches.push(row)
      }, () => {
        unlock()
        callback(branches)
      })
    }, allowableLockCount, 'Blockchain.getBranches()')
  }
  
  getBranchStructure(branchId, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      let currentBranchId = branchId
      const structure = [branchId]
      const readNext = () => {
        this.db.each("SELECT parentId FROM branches WHERE id=?", [currentBranchId], ({parentId}) => {
          if (parentId) {
            currentBranchId = parentId
            structure.push(parentId)
            setImmediate(() => {
              readNext()
            })
          } else {
            unlock()
            callback(structure)
          }
        })
      }
      
      readNext()
    }, allowableLockCount, 'Blockchain.getBranchStructure()')
  }
  
  getBranchStructureByBlockId(blockId, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.getBlockMetaById(blockId, (blockMeta) => {
        this.getBranchStructure(blockMeta.branchId, (branchStructure) => {
          unlock()
          callback(branchStructure)
        }, allowableLockCount + 1)
      }, allowableLockCount + 1)
    }, allowableLockCount, 'Blockchain.getBranchStructureByBlockId()')
  }
  
  restoreBlockIdFromTxHashSpentAt(txHash, spentAt, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.getBranchStructureByBlockId(spentAt, (branchStructure) => {
        this.findBlockRowWithTxInBranchStructure(txHash, branchStructure, (blockRow) => {
          unlock()
          callback(blockRow.id)
        }, allowableLockCount + 1)
      }, allowableLockCount + 1)
    }, allowableLockCount, 'Blockchain.getBlockIdByTxHashSpentAt()')
  }
  
  isBlockInBranchStructure(blockMeta, branchStructure, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      if (blockMeta.branchId === branchStructure[0]) {
        ret(true)
        return
      }
      
      const branchIndex = branchStructure.indexOf(blockMeta.branchId)
      if (branchIndex === -1) {
        ret(false)
        return
      }
      
      const childBranchId = branchStructure[branchIndex - 1]
      this.getBranchById(childBranchId, (childBranch) => {
        this.getBlockMetaById(childBranch.blockId, (meta) => {
          ret(blockMeta.height < meta.height)
        }, allowableLockCount + 1)
      }, allowableLockCount + 1)
    }, allowableLockCount, 'Blockchain.isBlockInBranchStructure()')
  }
  
  getBlockIdByHash(hash, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      this.db.each("SELECT id FROM blocks WHERE hash=? LIMIT 1", [hash], (row) => {
        ret(row.id)
      }, (rowCount) => {
        if (!rowCount) {
          ret(0)
        }
      })
    }, allowableLockCount, 'Blockchain.getBlockIdByHash()')
  }
  
  getBlockIdByHashForced(hash, callback) {
    this.db.each("SELECT id FROM blocks WHERE hash=? LIMIT 1", [hash], (row) => {
      callback(row.id)
    }, (rowCount) => {
      if (!rowCount) {
        callback(0)
      }
    })
  }
  
  getBlockMetaById(id, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      this.db.each("SELECT id, branchId, height, prevBlock, time, hash FROM blocks WHERE id=?", [id], (row) => {
        ret(row)
      }, (rowCount) => {
        if (!rowCount) {
          ret(null)
        }
      })
    }, allowableLockCount, 'Blockchain.getBlockMetaById()')
  }
  
  getBlockMetaByHash(hash, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      let returned = false
      this.db.each("SELECT id, branchId, height, prevBlock, time, hash FROM blocks WHERE hash=?", [hash], (row) => {
        if (returned) {
          storage.emit('fatalError', 'Same block hashes in blockchain')
        }
        returned = true
        ret(row)
      }, (rowCount) => {
        if (!rowCount) {
          ret(null)
        }
      })
    }, allowableLockCount, 'Blockchain.getBlockMetaByHash()')
  }
  
  getBlockRowById(id, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      this.db.each("SELECT * FROM blocks WHERE id=?", [id], (row) => {
        ret(row)
      }, (rowCount) => {
        if (!rowCount) {
          ret(null)
        }
      })
    }, allowableLockCount, 'Blockchain.getBlockRowById()')
  }
  
  getBlockRowByHash(hash, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      let returned = false
      this.db.each("SELECT * FROM blocks WHERE hash=?", [hash], (row) => {
        if (returned) {
          storage.emit('fatalError', 'Same block hashes in blockchain')
        }
        returned = true
        ret(row)
      }, (rowCount) => {
        if (!rowCount) {
          ret(null)
        }
      })
    }, allowableLockCount, 'Blockchain.getBlockRowByHash()')
  }
  
  getBlockByHash(hash, callback, allowableLockCount = 0) {
    this.getBlockRowByHash(hash, (row) => {
      callback(row ? this.rowToBlock(row) : null, row ? row.height : -1)
    }, allowableLockCount)
  }
  
  getBlockCountByHeight(height, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      this.db.each("SELECT COUNT(id) cnt FROM blocks WHERE height=?", [height], ({cnt}) => {
        ret(cnt)
      })
    }, allowableLockCount, 'Blockchain.getBlockCountByHeight()')
  }
  
  getBlockRowInBranchStructureByHeight(branchStructure, height, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      let found = false
      this.db.getAll("SELECT * FROM blocks WHERE height=? AND branchId IN (" + R.join(',', R.repeat('?', branchStructure.length)) + ") ORDER BY branchId DESC", [height, ...branchStructure])
        .then((blockRows) => Asyncs.forEach(blockRows, (blockRow, i, next) => {
          this.isBlockInBranchStructure(blockRow, branchStructure, (isIn) => {
            if (isIn) {
              unlock()
              callback(blockRow)
            } else {
              next()
            }
          }, allowableLockCount + 1)
        }, () => {
          unlock()
          callback(null)
        }))
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.getBlockRowInBranchStructureByHeight()')
  }
  
  getBlockRowInMasterBranchByHeight(height, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.getMasterBranch((masterBranch) => {
        this.getBranchStructure(masterBranch.id, (branchStructure) => {
          this.getBlockRowInBranchStructureByHeight(branchStructure, height, (blockRow) => {
            unlock()
            callback(blockRow, masterBranch)
          }, allowableLockCount + 1)
        }, allowableLockCount + 1)
      }, allowableLockCount + 1)
    }, allowableLockCount, 'Blockchain.getBlockRowInMasterBranchByHeight()')
  }
  
  // if blockchain contains only one branch
  findBlockIdWithTxInSingleBranch(hash, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.db.getRow("SELECT blockId FROM txs WHERE hash=? LIMIT 1", [hash])
        .then((row) => {
          unlock()
          callback(row ? row.blockId : 0)
        })
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.findBlockIdWithTxInSingleBranch()')
  }
  
  findBlockRowWithTxInBranchStructure(hash, branchStructure, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      let found = false
      // blocks.id ASC because there may be same tx hashes in different blocks
      this.db.getAll("SELECT txs.blockId FROM txs LEFT JOIN blocks ON txs.blockId=blocks.id WHERE txs.hash=? AND blocks.branchId IN (" + R.join(',', R.repeat('?', branchStructure.length)) + ") ORDER BY blocks.branchId DESC, blocks.id ASC", [hash, ...branchStructure])
        .then((blockIdRows) => Asyncs.forEach(blockIdRows, (blockIdRow, i, next) => {
          this.getBlockRowById(blockIdRow.blockId, (blockRow) => {
            this.isBlockInBranchStructure(blockRow, branchStructure, (isIn) => {
              if (isIn) {
                unlock()
                callback(blockRow)
              } else {
                next()
              }
            }, allowableLockCount + 1)
          }, allowableLockCount + 1)
        }, () => {
          unlock()
          callback(null)
        }))
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.findBlockRowWithTxInBranchStructure()')
  }
  
  getTxInBranchStructure(hash, branchStructure, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.findBlockRowWithTxInBranchStructure(hash, branchStructure, (blockRow) => {
        if (!blockRow) {
          unlock()
          callback(null)
          return
        }
        
        const block = this.rowToBlock(blockRow)
        const blockData = block.getData()
        for (const i in blockData.txList) {
          const tx = blockData.txList[i]
          if (tx.getHash().equals(hash)) {
            unlock()
            callback(tx)
            break
          }
        }
      }, allowableLockCount + 1)
    }, allowableLockCount, 'Blockchain.getTxInBranchStructure()')
  }
  
  checkForCollision(branchId, blockHeight, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      const ret = (res) => {
        unlock()
        callback(res)
      }
      
      this.db.each("SELECT COUNT(id) cnt FROM blocks WHERE branchId=? AND height=?", [branchId, blockHeight], ({cnt}) => {
        ret(!!cnt)
      })
    }, allowableLockCount, 'Blockchain.checkForCollision()')
  }
  
  getLength(callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      unlock()
      callback(this.length)
    }, allowableLockCount, 'Blockchain.getLength()')
  }
  
  getLengthForced(callback) {
    callback(this.length)
  }
  
  getCountByTimeInBranchStructure(branchStructure, since, till, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.db.getAll("SELECT id, branchId, height, prevBlock, time, hash FROM blocks WHERE time>=? AND time<=?", [since, till]).then((blocks) => {
        let cnt = 0
        Asyncs.forEach(blocks, (blockMeta, i, next) => {
          this.isBlockInBranchStructure(blockMeta, branchStructure, (isIn) => {
            if (isIn) {
              cnt++
            }
            next()
          }, allowableLockCount + 1)
        }, () => {
          unlock()
          callback(cnt)
        })
      })
    }, allowableLockCount, 'Blockchain.getCountByTimeInBranchStructure()')
  }
  
  txOutSpentInBranchStructure(hash, outN, branchStructure, callback, allowableLockCount = 0) {
    this.whenUnlocked((unlock) => {
      this.db.getAll("SELECT spentAt FROM spends WHERE txHash=? AND outN=?", [hash, outN])
        .then((spendRows) => Asyncs.forEach(spendRows, ({spentAt}, i, next) => {
          this.getBlockMetaById(spentAt, (blockMeta) => {
            this.isBlockInBranchStructure(blockMeta, branchStructure, (isIn) => {
              if (!isIn) {
                next()
                return
              }
              
              unlock()
              callback(spentAt)
            }, allowableLockCount + 1)
          }, allowableLockCount + 1)
        }, () => {
          unlock()
          callback(0)
        }))
        .catch((err) => storage.emit('fatalError', err))
    }, allowableLockCount, 'Blockchain.txOutSpentInBranchStructure()')
  }
  
  isTxOutSpentFreeTxs(hash, out) {
    const Tx = require('./Tx')
    return this.freeTxs.each(({hash: freeTxHash, data}) => {
      const tx = Tx.fromRaw(freeTxHash, data)
      return tx.getData().txIns.each(({txHash, outN}) => {
        if (txHash.equals(hash) && outN === out) {
          return true
        }
      }, false)
    }, false)
  }
  
  deleteOldFreeTxs() {
    const minLocalTime = Time.local() - 600
    this.freeTxs.filter(({hash, added}) => {
      const isOld = added < minLocalTime
      if (isOld) {
        this.onFreeTxDeleted(hash)
      }
      return !isOld
    })
  }
  
  isFreeTxKnown(txHash) {
    this.deleteOldFreeTxs()
    return this.freeTxs.indexOf('hash', txHash) >= 0
  }
  
  addFreeTx(tx) {
    this.deleteOldFreeTxs()
    this.freeTxs.push({hash: tx.getHash(), data: tx.getRawData(), added: Time.local()}, {data: tx.getRawDataLength()})
    this.onFreeTxAdded(tx)
    this.emit('changed')
  }
  
  deleteFreeTx(txHash) {
    const index = this.freeTxs.indexOf('hash', txHash)
    if (index >= 0) {
      this.freeTxs.remove(index)
      this.onFreeTxDeleted(txHash)
      this.emit('changed')
      return true
    } else {
      return false
    }
  }
  
  eachFreeTx(itemCallback, returnCallback) {
    this.freeTxs.clone().eachAsync(itemCallback, returnCallback)
  }
  
  rEachFreeTx(itemCallback, returnCallback) {
    this.freeTxs.clone().rEachAsync(itemCallback, returnCallback)
  }
  
  saveCheckpoint(callback) {
    const name = storage.lastCheckpoint && storage.lastCheckpoint === '1' ? '2' : '1'
    const path = PATH_CHECKPOINTS + name + '/'
    this.whenUnlocked((unlock) => {
      Files.removeDir(path)
        .then(() => {
          Files.needDir(path, () => {
            disp.lockTerm()
            new Promise((resolve) => {
              this.eachPlugin((instance, next) => {
                if (instance.onBeforeSaveCheckpoint) {
                  instance.onBeforeSaveCheckpoint(next)
                } else {
                  next()
                }
              }, () => {
                resolve()
              })
            })
              .then(() => this.db.saveCheckpoint(path))
              .then(() => new Promise((resolve) => {
                this.eachPlugin((instance, next) => {
                  if (instance.onSaveCheckpoint) {
                    instance.onSaveCheckpoint(path, next)
                  } else {
                    next()
                  }
                }, () => {
                  resolve()
                })
              }))
              .then(() => Files.touch(path + 'ready'))
              .then(() => {
                storage.lastCheckpoint = name
                storage.flush(() => {
                  disp.unlockTerm()
                  unlock()
                  this.log('{green-fg}Checkpoint ' + name + ' saved{/green-fg}')
                  callback && callback()
                })
              })
              .catch((err) => storage.emit('fatalError', err))
          })
        })
        .catch((err) => storage.emit('fatalError', err))
    }, 0, 'Blockchain.saveCheckpoint()')
  }
  
  loadCheckpoint(callback) {
    if (!storage.lastCheckpoint) {
      callback && callback(false)
      return
    }
    
    const name = storage.lastCheckpoint
    const path = PATH_CHECKPOINTS + name + '/'
    if (fs.existsSync(path + 'ready')) {
      disp.lockTerm()
      this.db.loadCheckpoint(path)
        .then(() => new Promise((resolve) => {
          this.eachPlugin((instance, next) => {
            if (instance.onLoadCheckpoint) {
              instance.onLoadCheckpoint(path, next)
            } else {
              next()
            }
          }, () => {
            resolve()
          })
        }))
        .then(() => {
          storage.blockchainCached = true
          storage.flush(() => {
            disp.unlockTerm()
            callback && callback(true)
          })
        })
        .catch((err) => storage.emit('fatalError', err))
    } else {
      callback && callback(false)
    }
  }
  
  getDb() {
    return this.db
  }
  
  getInitialPrevBlock() {
    return INITIAL_PREV_BLOCK
  }
  
  getFirstBlockHash() {
    return FIRST_BLOCK_HASH
  }
}

const blockchain = new Blockchain
module.exports = blockchain