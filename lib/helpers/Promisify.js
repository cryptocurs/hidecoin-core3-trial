'use strict'

module.exports = (func, callbackArgNum) => (...args) => new Promise((resolve) => {
  args[callbackArgNum] = (callbackFirstArg) => {
    resolve(callbackFirstArg)
  }
  func(...args)
})