'use strict'

const moment = require('moment')
const R = require('ramda')
const EventEmitter = require('events')

const disp = require('./Disp')
const storage = require('./Storage')

module.exports = class Component extends EventEmitter {

  constructor() {
    super()
    this.module = 'UNK'
    this.locks = 0
    this.lockQueueLength = 0
    
    this.isLocked = () => {
      return this.locks > 0
    }
    
    this.log = (...data) => {
      this.logBy(this.module, ...data)
    }
    
    this.logBy = (module, ...data) => {
      if (!storage.session.disableLog && (!storage.logIgnoreModules || !storage.logIgnoreModules[module]) && (!storage.logTrackModule || storage.logTrackModule === module)) {
        const dataTimed = ['[' + moment().format('HH:mm:ss') + ' ' + module + ']#', ...data]
        const dataToLog = R.contains(module, ['FND', 'WLT', 'COL']) ? [module, ...dataTimed] : dataTimed
        storage.emit('log', ...dataToLog) || console.log(...dataToLog)
      }
    }
    
    this.logAlias = (alias, data) => {
      this.logAliasBy(this.module, alias, data)
    }
    
    this.logAliasBy = (module, alias, data) => {
      if (!storage.session.disableLog) {
        storage.emit('logAlias', module, alias, data) || console.log(data)
      }
    }
    
    this.logAliasClear = (alias) => {
      storage.emit('logAliasClear', this.module, alias)
    }
    
    if (!storage.session.lockQueue) {
      storage.session.lockQueue = []
    }
  }
  
  lock(times = 1) {
    this.locks += times
  }
  
  unlock(times = 1) {
    this.locks -= times
  }
  
  whenUnlocked(callback, allowableLockCount = 0, description = null, repeat = false) {
    !repeat && description && this.logBy('LCK', 'Locking by', description, 'with', this.locks, '/', allowableLockCount)
    
    this.lockQueueLength++
    if (this.locks > allowableLockCount) {
      setTimeout(() => {
        this.lockQueueLength--
        this.whenUnlocked(callback, allowableLockCount, description, true)
      }, 10)
    } else {
      description && storage.session.lockQueue.push(description)
      let unlocked = false
      this.lock()
      this.lockQueueLength--
      description && this.logBy('LCK', 'Locked by', description, 'with', this.locks, '/', allowableLockCount)
      callback(() => {
        if (unlocked) {
          storage.emit('fatalError', 'Double unlock by ' + description)
        }
        unlocked = true
        this.unlock()
        if (this.locks < 0) {
          storage.emit('fatalError', 'Excess unlock')
        }
        if (description) {
          this.logBy('LCK', 'Unlocked by', description, 'with', this.locks, '/', allowableLockCount)
          const index = storage.session.lockQueue.indexOf(description)
          if (index >= 0) {
            storage.session.lockQueue.splice(index, 1)
          } else {
            storage.emit('fatalError', 'Already unlocked by ' + description)
          }
        }
      })
    }
  }
  
  getLockQueueLength() {
    return this.lockQueueLength
  }
}