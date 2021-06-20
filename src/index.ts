import { Db } from 'mongodb'
import logger from 'debug'
const log = logger('sourced-repo-mongo')
import _ from 'lodash'

export function Repository(
  entityType,
  options: { db: Db; indices?: string[]; snapshotFrequency?: number }
): void {
  options = options || { db: null }
  // EventEmitter.call(this);
  if (!options.db) {
    throw new Error(
      "mongo has not been initialized. you must call require('sourced-repo-mongo/mongo').connect(config.MONGO_URL); before instantiating a Repository"
    )
  }
  const indices = _.union(options.indices, ['id'])
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this
  const db = options.db
  self.entityType = entityType
  self.indices = indices
  self.snapshotFrequency = options.snapshotFrequency || 10

  const snapshotCollectionName = `${entityType.name}.snapshots`
  const snapshots = db.collection(snapshotCollectionName)
  self.snapshots = snapshots
  const eventCollectionName = `${entityType.name}.events`
  const events = db.collection(eventCollectionName)
  self.events = events

  self.indices.forEach(async function (index) {
    await snapshots.createIndex(index)
    await events.createIndex(index)
  })
  events.createIndex({ id: 1, version: 1 }).then(async () => {
    await snapshots.createIndex({ id: 1, version: 1 })
    await snapshots.createIndex('snapshotVersion')
  })

  log('initialized %s entity store', self.entityType.name)
}

Repository.prototype.commit = async function commit(
  entity,
  options,
  cb
): Promise<void> {
  if (typeof options === 'function') {
    cb = options
    options = {}
  }

  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  log('committing %s for id %s', this.entityType.name, entity.id)

  this._commitEvents(entity, function _afterCommitEvents(err) {
    if (err) return cb(err)

    self._commitSnapshots(entity, options, function _afterCommitSnapshots(err) {
      if (err) return cb(err)
      self._emitEvents(entity)
      return cb()
    })
  })
}

Repository.prototype.commitAll = function commit(entities, options, cb) {
  if (typeof options === 'function') {
    cb = options
    options = {}
  }

  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  log('committing %s for id %j', this.entityType.name, _.map(entities, 'id'))

  this._commitAllEvents(entities, function _afterCommitEvents(err) {
    if (err) return cb(err)
    self._commitAllSnapshots(
      entities,
      options,
      function _afterCommitSnapshots(err) {
        if (err) return cb(err)
        entities.forEach(function (entity) {
          self._emitEvents(entity)
        })
        return cb()
      }
    )
  })
}

Repository.prototype.get = function get(id, cb) {
  return this._getByIndex('id', id, cb)
}

Repository.prototype._getByIndex = function _getByIndex(index, value, cb) {
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  if (this.indices.indexOf(index) === -1)
    throw new Error(
      `Cannot get sourced entity type ${this.entityType} by index ${index}`
      // this.entityType,
      // index
    )

  log('getting %s for %s %s', this.entityType.name, index, value)

  const criteria: { version?: any; $or?: any } = {}
  criteria[index] = value

  this.snapshots
    .find(criteria)
    .sort({ version: -1 })
    .limit(-1)
    .toArray(function (err, snapshots) {
      if (err) return cb(err)

      const snapshot = snapshots[0]

      if (snapshot) criteria.version = { $gt: snapshot.version }
      self.events
        .find(criteria)
        .sort({ version: 1 })
        .toArray(function (err, events) {
          if (err) return cb(err)
          if (snapshot) delete snapshot._id
          if (!snapshot && !events.length) return cb(null, null)

          const id =
            index === 'id' ? value : snapshot ? snapshot.id : events[0].id

          const entity = self._deserialize(id, snapshot, events)
          return cb(null, entity)
        })
    })
}

Repository.prototype.getAll = function getAll(ids, cb) {
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  if (typeof ids === 'function') {
    cb = ids
    ids = null
    return this.events.distinct('id', function (err, distinctEventIds) {
      self.getAll(distinctEventIds, cb)
    })
  }

  log('getting %ss for ids %j', this.entityType.name, ids)

  if (ids.length === 0) return cb(null, [])

  this._getAllSnapshots(ids, function _afterGetAllSnapshots(err, snapshots) {
    if (err) return cb(err)
    self._getAllEvents(ids, snapshots, function (err, entities) {
      if (err) return cb(err)
      return cb(null, entities)
    })
  })
}

Repository.prototype._commitEvents = function _commitEvents(entity, cb) {
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  if (entity.newEvents.length === 0) return cb()

  if (!entity.id)
    return cb(
      new Error(
        `Cannot commit an entity of type ${this.entityType} without an [id] property`
        // this.entityType
      )
    )

  const events = entity.newEvents
  events.forEach(function _applyIndices(event) {
    if (event && event._id) delete event._id // mongo will blow up if we try to insert multiple _id keys
    self.indices.forEach(function (index) {
      event[index] = entity[index]
    })
  })
  self.events.insertMany(events, function (err) {
    if (err) return cb(err)
    log('committed %s.events for id %s', self.entityType.name, entity.id)
    entity.newEvents = []
    return cb()
  })
}

Repository.prototype._commitAllEvents = function _commitEvents(entities, cb) {
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  const events = []
  entities.forEach(function (entity) {
    if (entity.newEvents.length === 0) return

    if (!entity.id)
      return cb(
        new Error(
          `Cannot commit an entity of type ${self.entityType} without an [id] property`
          // self.entityType
        )
      )

    const evnts = entity.newEvents
    evnts.forEach(function _applyIndices(event) {
      if (event && event._id) delete event._id // mongo will blow up if we try to insert multiple _id keys
      self.indices.forEach(function (index) {
        event[index] = entity[index]
      })
    })
    Array.prototype.unshift.apply(events, evnts)
  })

  if (events.length === 0) return cb()

  self.events.insertMany(events, function (err) {
    if (err) return cb(err)
    log(
      'committed %s.events for ids %j',
      self.entityType.name,
      _.map(entities, 'id')
    )
    entities.forEach(function (entity) {
      entity.newEvents = []
    })
    return cb()
  })
}

Repository.prototype._commitSnapshots = function _commitSnapshots(
  entity,
  options,
  cb
) {
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  if (
    options.forceSnapshot ||
    entity.version >= entity.snapshotVersion + self.snapshotFrequency
  ) {
    const snapshot = entity.snapshot()
    if (snapshot && snapshot._id) delete snapshot._id // mongo will blow up if we try to insert multiple _id keys
    self.snapshots.insertOne(snapshot, function (err) {
      if (err) return cb(err)
      log(
        'committed %s.snapshot for id %s %j',
        self.entityType.name,
        entity.id,
        snapshot
      )
      return cb(null, entity)
    })
  } else {
    return cb(null, entity)
  }
}

Repository.prototype._commitAllSnapshots = function _commitAllSnapshots(
  entities,
  options,
  cb
) {
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  const snapshots = []
  entities.forEach(function (entity) {
    if (
      options.forceSnapshot ||
      entity.version >= entity.snapshotVersion + self.snapshotFrequency
    ) {
      const snapshot = entity.snapshot()
      if (snapshot) {
        if (snapshot._id) delete snapshot._id // mongo will blow up if we try to insert multiple _id keys)
        snapshots.push(snapshot)
      }
    }
  })

  if (snapshots.length === 0) return cb()

  self.snapshots.insertMany(snapshots, function (err) {
    if (err) return cb(err)
    log(
      'committed %s.snapshot for ids %s %j',
      self.entityType.name,
      _.map(entities, 'id'),
      snapshots
    )
    return cb(null, entities)
  })
}

Repository.prototype._deserialize = function _deserialize(
  id,
  snapshot,
  events
) {
  log('deserializing %s entity ', this.entityType.name)
  const entity = new this.entityType(snapshot, events)
  entity.id = id
  return entity
}

Repository.prototype._emitEvents = function _emitEvents(entity) {
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  const eventsToEmit = entity.eventsToEmit
  entity.eventsToEmit = []
  eventsToEmit.forEach(function (eventToEmit) {
    const args = Array.prototype.slice.call(eventToEmit)
    self.entityType.prototype.emit.apply(entity, args)
  })

  log('emitted local events for id %s', entity.id)
}

Repository.prototype._getAllSnapshots = function _getAllSnapshots(ids, cb) {
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  const match = { $match: { id: { $in: ids } } }
  const sort = { $sort: { snapshotVersion: 1 } }
  const group = {
    $group: { _id: '$id', snapshotVersion: { $last: '$snapshotVersion' } }
  }

  self.snapshots.aggregate([match, sort, group], function (err, cursor) {
    if (err) return cb(err)
    cursor.toArray(function (err, idVersionPairs) {
      if (err) return cb(err)
      let criteria: { $or?: any; id?: any; snapshotVersion?: any } = {}
      if (idVersionPairs.length === 0) {
        return cb(null, [])
      } else if (idVersionPairs.length === 1) {
        criteria = {
          id: idVersionPairs[0]._id,
          snapshotVersion: idVersionPairs[0].snapshotVersion
        }
      } else {
        criteria.$or = []
        idVersionPairs.forEach(function (pair) {
          const cri = { id: pair._id, snapshotVersion: pair.snapshotVersion }
          criteria.$or.push(cri)
        })
      }
      self.snapshots.find(criteria).toArray(function (err, snapshots) {
        if (err) cb(err)
        return cb(null, snapshots)
      })
    })
  })
}

Repository.prototype._getAllEvents = function _getAllEvents(
  ids,
  snapshots,
  cb
) {
  // eslint-disable-next-line @typescript-eslint/no-this-alias
  const self = this

  const criteria = { $or: [] }
  ids.forEach(function (id) {
    let snapshot
    if (
      !(snapshot = _.find(snapshots, function (snapshot) {
        return id === snapshot.id
      }))
    ) {
      criteria.$or.push({ id: id })
    } else {
      criteria.$or.push({
        id: snapshot.id,
        version: { $gt: snapshot.snapshotVersion }
      })
    }
  })

  self.events
    .find(criteria)
    .sort({ id: 1, version: 1 })
    .toArray(function (err, events) {
      if (err) return cb(err)
      if (!snapshots.length && !events.length) return cb(null, null)
      const results = []
      ids.forEach(function (id) {
        const snapshot = _.find(snapshots, function (snapshot) {
          return snapshot.id === id
        })
        if (snapshot) delete snapshot._id
        const evnts = _.filter(events, function (event) {
          return event.id === id
        })
        const entity = self._deserialize(id, snapshot, evnts)
        results.push(entity)
      })
      return cb(null, results)
    })
}

// module.exports.Repository = Repository;
