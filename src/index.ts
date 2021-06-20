import { Db } from 'mongodb'
import logger from 'debug'
const log = logger('sourced-repo-mongo')
import _ from 'lodash'

export class Repository {
  entityType: any
  indices: string[]
  snapshotFrequency: number
  snapshots: any
  events: any
  constructor(
    entityType,
    options: { db: Db; indices?: string[]; snapshotFrequency?: number }
  ) {
    options = options || { db: null }
    // EventEmitter.call(this);
    if (!options.db) {
      throw new Error(
        "mongo has not been initialized. you must call require('sourced-repo-mongo/mongo').connect(config.MONGO_URL); before instantiating a Repository"
      )
    }
    const indices = _.union(options.indices, ['id'])
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    // const self = this
    const db = options.db
    this.entityType = entityType
    this.indices = indices
    this.snapshotFrequency = options.snapshotFrequency || 10

    const snapshotCollectionName = `${entityType.name}.snapshots`
    const snapshots = db.collection(snapshotCollectionName)
    this.snapshots = snapshots
    const eventCollectionName = `${entityType.name}.events`
    const events = db.collection(eventCollectionName)
    this.events = events
  }

  async init(): Promise<void> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    this.indices.forEach(async (index) => {
      await this.snapshots.createIndex(index)
      await this.events.createIndex(index)
    })
    this.events
      .createIndex({ id: 1, version: 1 }, { unique: true, background: false })
      .then(async (index) => {
        console.log('index created', index)
        await this.snapshots.createIndex(
          { id: 1, version: 1 },
          { unique: true, background: false }
        )
        await this.snapshots.createIndex('snapshotVersion')
      })

    log('initialized %s entity store', this.entityType.name)
  }

  commit(entity, options?, cb?) {
    if (typeof options === 'function') {
      cb = options
      options = {}
    }

    log('committing %s for id %s', this.entityType.name, entity.id)

    this._commitEvents(entity, (err) => {
      if (err) return cb(err)

      this._commitSnapshots(entity, options, (err) => {
        if (err) return cb(err)
        this._emitEvents(entity)
        return cb()
      })
    })
  }

  commitAll(entities, options?, cb?) {
    if (typeof options === 'function') {
      cb = options
      options = {}
    }

    log('committing %s for id %j', this.entityType.name, _.map(entities, 'id'))

    this._commitAllEvents(entities, (err) => {
      if (err) return cb(err)
      this._commitAllSnapshots(entities, options, (err) => {
        if (err) return cb(err)
        entities.forEach((entity) => {
          this._emitEvents(entity)
        })
        return cb()
      })
    })
  }

  get(id, cb) {
    return this._getByIndex('id', id, cb)
  }

  _getByIndex(index, value, cb) {
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
      .toArray((err, snapshots) => {
        if (err) return cb(err)

        const snapshot = snapshots[0]

        if (snapshot) criteria.version = { $gt: snapshot.version }
        self.events
          .find(criteria)
          .sort({ version: 1 })
          .toArray((err, events) => {
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

  getAll(ids?, cb?) {
    if (typeof ids === 'function') {
      cb = ids
      ids = null
      return this.events.distinct('id', (err, distinctEventIds) => {
        this.getAll(distinctEventIds, cb)
      })
    }

    log('getting %ss for ids %j', this.entityType.name, ids)

    if (ids.length === 0) return cb(null, [])

    this._getAllSnapshots(ids, (err, snapshots) => {
      if (err) return cb(err)
      this._getAllEvents(ids, snapshots, (err, entities) => {
        if (err) return cb(err)
        return cb(null, entities)
      })
    })
  }

  _commitEvents(entity, cb) {
    if (entity.newEvents.length === 0) return cb()

    if (!entity.id)
      return cb(
        new Error(
          `Cannot commit an entity of type ${this.entityType} without an [id] property`
          // this.entityType
        )
      )

    const events = entity.newEvents
    events.forEach((event) => {
      if (event && event._id) delete event._id // mongo will blow up if we try to insert multiple _id keys
      this.indices.forEach((index) => {
        event[index] = entity[index]
      })
    })
    this.events.insertMany(events, (err) => {
      if (err) return cb(err)
      log('committed %s.events for id %s', this.entityType.name, entity.id)
      entity.newEvents = []
      return cb()
    })
  }

  _commitAllEvents(entities, cb) {
    const events = []
    entities.forEach((entity) => {
      if (entity.newEvents.length === 0) return

      if (!entity.id)
        return cb(
          new Error(
            `Cannot commit an entity of type ${this.entityType} without an [id] property`
            // self.entityType
          )
        )

      const evnts = entity.newEvents
      evnts.forEach((event) => {
        if (event && event._id) delete event._id // mongo will blow up if we try to insert multiple _id keys
        this.indices.forEach((index) => {
          event[index] = entity[index]
        })
      })
      Array.prototype.unshift.apply(events, evnts)
    })

    if (events.length === 0) return cb()

    this.events.insertMany(events, (err) => {
      if (err) return cb(err)
      log(
        'committed %s.events for ids %j',
        this.entityType.name,
        _.map(entities, 'id')
      )
      entities.forEach((entity) => {
        entity.newEvents = []
      })
      return cb()
    })
  }

  _commitSnapshots(entity, options, cb) {
    if (
      options.forceSnapshot ||
      entity.version >= entity.snapshotVersion + this.snapshotFrequency
    ) {
      const snapshot = entity.snapshot()
      if (snapshot && snapshot._id) delete snapshot._id // mongo will blow up if we try to insert multiple _id keys
      this.snapshots.insertOne(snapshot, (err) => {
        if (err) return cb(err)
        log(
          'committed %s.snapshot for id %s %j',
          this.entityType.name,
          entity.id,
          snapshot
        )
        return cb(null, entity)
      })
    } else {
      return cb(null, entity)
    }
  }

  _commitAllSnapshots(entities, options, cb) {
    const snapshots = []
    entities.forEach((entity) => {
      if (
        options.forceSnapshot ||
        entity.version >= entity.snapshotVersion + this.snapshotFrequency
      ) {
        const snapshot = entity.snapshot()
        if (snapshot) {
          if (snapshot._id) delete snapshot._id // mongo will blow up if we try to insert multiple _id keys)
          snapshots.push(snapshot)
        }
      }
    })

    if (snapshots.length === 0) return cb()

    this.snapshots.insertMany(snapshots, (err) => {
      if (err) return cb(err)
      log(
        'committed %s.snapshot for ids %s %j',
        this.entityType.name,
        _.map(entities, 'id'),
        snapshots
      )
      return cb(null, entities)
    })
  }

  _deserialize(id, snapshot, events) {
    log('deserializing %s entity ', this.entityType.name)
    const entity = new this.entityType(snapshot, events)
    entity.id = id
    return entity
  }

  _emitEvents(entity) {
    const eventsToEmit = entity.eventsToEmit
    entity.eventsToEmit = []
    eventsToEmit.forEach((eventToEmit) => {
      const args = Array.prototype.slice.call(eventToEmit)
      this.entityType.prototype.emit.apply(entity, args)
    })

    log('emitted local events for id %s', entity.id)
  }

  _getAllSnapshots(ids, cb) {
    const match = { $match: { id: { $in: ids } } }
    const sort = { $sort: { snapshotVersion: 1 } }
    const group = {
      $group: { _id: '$id', snapshotVersion: { $last: '$snapshotVersion' } }
    }

    this.snapshots.aggregate([match, sort, group], (err, cursor) => {
      if (err) return cb(err)
      cursor.toArray((err, idVersionPairs) => {
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
          idVersionPairs.forEach((pair) => {
            const cri = { id: pair._id, snapshotVersion: pair.snapshotVersion }
            criteria.$or.push(cri)
          })
        }
        this.snapshots.find(criteria).toArray((err, snapshots) => {
          if (err) cb(err)
          return cb(null, snapshots)
        })
      })
    })
  }

  _getAllEvents(ids, snapshots, cb) {
    const criteria = { $or: [] }
    ids.forEach((id) => {
      let snapshot
      if (!(snapshot = _.find(snapshots, (snapshot) => id === snapshot.id))) {
        criteria.$or.push({ id: id })
      } else {
        criteria.$or.push({
          id: snapshot.id,
          version: { $gt: snapshot.snapshotVersion }
        })
      }
    })

    this.events
      .find(criteria)
      .sort({ id: 1, version: 1 })
      .toArray((err, events) => {
        if (err) return cb(err)
        if (!snapshots.length && !events.length) return cb(null, null)
        const results = []
        ids.forEach((id) => {
          const snapshot = _.find(snapshots, (snapshot) => snapshot.id === id)
          if (snapshot) delete snapshot._id
          const evnts = _.filter(events, (event) => event.id === id)
          const entity = this._deserialize(id, snapshot, evnts)
          results.push(entity)
        })
        return cb(null, results)
      })
  }
}

// module.exports.Repository = Repository;
