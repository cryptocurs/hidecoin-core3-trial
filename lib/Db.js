'use strict'

const R = require('ramda')
const sqlite = require('sqlite3').verbose()

const {Asyncs, Files} = require('./helpers')
const Component = require('./Component')
const storage = require('./Storage')

class Db extends Component {

  constructor(config) {
    super()
    this.module = 'SQL'
    this.bigQueries = {}
    this.basePath = config.basePath
    this.fileName = config.fileName
    this.transactions = 0
    this.dbPath = this.basePath + this.fileName
    this.db = new sqlite.Database(this.dbPath)
    this.log('Runned')
  }
  
  query(...args) {
    return new Promise((resolve) => {
      this.db.run(...args, function(err, res) {
        if (err) {
          storage.emit('fatalError', 'SQLite error: ' + err)
        }
        resolve(this.lastID, this.changes)
      })
    })
    .catch((err) => storage.emit('fatalError', err))
  }
  
  prepare(text) {
    return new Promise((resolve) => {
      const stmt = db.prepare(text)
      resolve(stmt, () => {
        stmt.finalize()
      })
    })
    .catch((err) => storage.emit('fatalError', err))
  }
  
  run(stmt, values) {
    return new Promise((resolve) => {
      stmt.run(values, (err) => {
        if (err) {
          storage.emit('fatalError', 'SQLite error: ' + err)
        }
        resolve()
      })
    })
    .catch((err) => storage.emit('fatalError', err))
  }
  
  each(...args) {
    const withValues = args[1] instanceof Array
    const text = args[0]
    const values = withValues ? args[1] : []
    const itemCallback = withValues ? args[2] : args[1]
    const returnCallback = withValues ? args[3] : args[2]
    
    this.db.each(text, values, (err, row) => {
      if (err) {
        storage.emit('fatalError', 'SQLite error: ' + err)
      }
      itemCallback(row)
    }, (err, rowCount) => {
      if (err) {
        storage.emit('fatalError', 'SQLite error: ' + err)
      }
      returnCallback && returnCallback(rowCount)
    })
  }
  
  getRow(...args) {
    return new Promise((resolve) => {
      this.db.each(...args, (err, row) => {
        if (err) {
          storage.emit('fatalError', 'SQLite error: ' + err)
        }
        resolve(row)
      }, (err, rowCount) => {
        if (err) {
          storage.emit('fatalError', 'SQLite error: ' + err)
        }
        if (!rowCount) {
          resolve(null)
        }
      })
    })
    .catch((err) => storage.emit('fatalError', err))
  }
  
  getAll(...args) {
    return new Promise((resolve) => {
      const rows = []
      this.db.each(...args, (err, row) => {
        if (err) {
          storage.emit('fatalError', 'SQLite error: ' + err)
        }
        rows.push(row)
      }, (err, rowCount) => {
        if (err) {
          storage.emit('fatalError', 'SQLite error: ' + err)
        }
        resolve(rows)
      })
    })
    .catch((err) => storage.emit('fatalError', err))
  }
  
  begin() {
    return this.transactions++ ? new Promise(resolve => { resolve() }) : this.query("BEGIN")
  }
  
  commit() {
    return --this.transactions ? new Promise(resolve => { resolve() }) : this.query("COMMIT")
  }
  
  bigQueryStart(text, tail, delimiter) {
    this.bigQueries[text] = {
      delimiter,
      tail,
      queries: [],
      values: []
    }
  }
  
  bigQueryRun(text) {
    return new Promise((resolve) => {
      const {delimiter, queries, values} = this.bigQueries[text]
      if (queries.length) {
        const bigText = text + R.join(delimiter, queries)
        const valuesCopy = values.slice()
        queries.length = 0
        values.length = 0
        db.query(bigText, valuesCopy)
          .then(() => resolve())
          .catch((err) => storage.emit('fatalError', err))
      } else {
        resolve()
      }
    })
    .catch((err) => storage.emit('fatalError', err))
  }
  
  bigQueryRunAll() {
    return new Promise((resolve) => {
      Asyncs.forEach(this.bigQueries, (bigQuery, text, next) => {
        this.bigQueryRun(text)
          .then(() => next())
          .catch((err) => storage.emit('fatalError', err))
      }, () => resolve())
    })
    .catch((err) => storage.emit('fatalError', err))
  }
  
  bigQueryPush(text, values) {
    return new Promise((resolve) => {
      const bigQuery = this.bigQueries[text]
      bigQuery.queries.push(bigQuery.tail)
      bigQuery.values = [...bigQuery.values, ...values]
      if (bigQuery.queries.length >= 128) {
        this.bigQueryRun(text)
          .then(() => resolve())
          .catch((err) => storage.emit('fatalError', err))
      } else {
        resolve()
      }
    })
    .catch((err) => storage.emit('fatalError', err))
  }
  
  bigQueryEnd(text) {
    return new Promise((resolve) => {
      this.bigQueryRun(text)
        .then(() => {
          delete this.bigQueries[text]
          resolve()
        })
        .catch((err) => storage.emit('fatalError', err))
    })
  }
  
  saveCheckpoint(path) {
    return new Promise((resolve) => {
      Files.copy(this.dbPath, path + this.fileName)
        .then(() => resolve())
        .catch((err) => storage.emit('fatalError', err))
    })
  }
  
  loadCheckpoint(path) {
    return new Promise((resolve) => {
      Files.copyBack(this.dbPath, path + this.fileName)
        .then(() => {
          this.db = new sqlite.Database(this.dbPath)
          resolve()
        })
        .catch((err) => storage.emit('fatalError', err))
    })
  }
}

module.exports = (config) => {
  return new Db(config)
}