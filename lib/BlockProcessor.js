'use strict'

const {Conv, Time} = require('./helpers')
const disp = require('./Disp')
const storage = require('./Storage')
const Component = require('./Component')
const blockchain = require('./Blockchain')
const Block = require('./Block')

class BlockProcessor extends Component {

  constructor() {
    super()
    this.module = 'BLP'
    
    this.broadcast = (hash, data) => {
      const net = require('./Net')
      net.broadcastBlockFoundZipped(hash, data)
    }
  }
  
  add(hash, rawData, module = null) {
    blockchain.getBlockIdByHash(hash, (blockId) => {
      if (!blockId) {
        const block = Block.fromRaw(hash, rawData)
        if (!block.wasUnpacked()) {
          return
        }
        
        blockchain.whenUnlocked((unlock) => {
          const blockHash = block.getHash()
          const blockData = block.getData()
          blockchain.getBlockIdByHash(blockHash, (blockId) => {
            if (blockId) { // KNOWN
              unlock()
              return
            }
            
            const addBlock = (prevBlockHash, prevBlockMeta) => {
              blockchain.getMasterBranch(({id}) => {
                blockchain.getBranchStructure(id, (branchStructure) => {
                  block.isValidInBranchStructure(branchStructure, (valid, err) => {
                    if (!valid) {
                      this.log('{red-fg}Block is NOT valid: ' + err + '{/red-fg}')
                      unlock()
                      return
                    }
                    
                    const blockHeight = prevBlockMeta.height + 1
                    blockchain.checkForCollision(prevBlockMeta.branchId, blockHeight, (collision) => {
                      let branchId = prevBlockMeta.branchId
                      
                      const addBlockToBranch = () => {
                        storage.session.synchronizer.lastBlockAdded = Time.local()
                        blockchain.addBlockToBranch(branchId, blockHeight, block, () => {
                          this.logBy(module || this.module, '{green-fg}New block ACCEPTED{/green-fg} and added to branch #' + branchId)
                          onChanged()
                          unlock()
                          
                          this.broadcast(hash, rawData)
                        }, 3)
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
                            addBlockToBranch()
                          }, 3)
                        } else {
                          addBlockToBranch()
                        }
                      }, 1)
                    }, 1)
                  }, 1)
                }, 1)
              }, 1)
            }
            
            blockchain.getBlockMetaByHash(blockData.prevBlock, (meta) => {
              if (meta) {
                addBlock(blockData.prevBlock, meta)
              } else {
                this.log('{red-fg}New block IGNORED: unknown prevBlock{/red-fg}')
                unlock()
              }
            }, 1)
          }, 1)
        }, 0, 'BlockProcessor.add()')
      }
    })
  }
}

const blockProcessor = new BlockProcessor
module.exports = blockProcessor