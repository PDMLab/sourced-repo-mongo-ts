import { Collection, Db, ObjectId } from 'mongodb'
import logger from 'debug'
const log = logger('sourced-repo-mongo')
import _ from 'lodash'
import { SourcedEntity } from 'sourced-ts'

type Snapshot = SourcedEntity & {
  _id: ObjectId
}

type Entity = SourcedEntity & {
  id: string
}

export class Repository {
  entityType: (snapshot, events) => void
  indices: any[]
  snapshotFrequency: number
  snapshots: Collection<any>
  events: Collection<any>
  constructor(
    entityType,
    options: { db: Db; indices?: string[]; snapshotFrequency?: number }
  ) {
    options = options || { db: null }
    if (!options.db) {
      throw new Error(
        "mongo has not been initialized. you must call require('sourced-repo-mongo/mongo').connect(config.MONGO_URL); before instantiating a Repository"
      )
    }
    const indices = _.union(options.indices, ['id'])
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
    this.indices.forEach(async (index) => {
      await this.snapshots.createIndex(index)
      await this.events.createIndex(index)
    })
    await this.events.createIndex(
      { id: 1, version: 1 },
      { unique: true, background: false }
    )
    await this.snapshots.createIndex(
      { id: 1, version: 1 },
      { unique: true, background: false }
    )
    await this.snapshots.createIndex('snapshotVersion')

    log('initialized %s entity store', this.entityType.name)
  }

  async commit(entity, options?): Promise<void> {
    if (!options) {
      options = {}
    }

    log('committing %s for id %s', this.entityType.name, entity.id)

    await this._commitEvents(entity)

    await this._commitSnapshots(entity, options)
    await this._emitEvents(entity)
  }

  async commitAll(entities: Entity[], options?): Promise<void> {
    if (!options) {
      options = {}
    }

    log('committing %s for id %j', this.entityType.name, _.map(entities, 'id'))

    await this._commitAllEvents(entities)
    await this._commitAllSnapshots(entities, options)
    entities.forEach((entity) => {
      this._emitEvents(entity)
    })
  }

  async getAllEvents(options?: { batchSize?: number }): Promise<unknown[]> {
    const events: unknown[] = []
    const { batchSize } = options ? options : { batchSize: 1000 }
    await this.events
      .find()
      .sort({ id: 1, version: 1 })
      .allowDiskUse()
      .batchSize(batchSize)
      .forEach((d) => {
        events.push(d)
      })
    return events
  }

  async get(id): Promise<any> {
    return this._getByIndex('id', id)
  }

  async _getByIndex(index, value): Promise<any> {
    if (this.indices.indexOf(index) === -1)
      throw new Error(
        `Cannot get sourced entity type ${this.entityType} by index ${index}`
      )

    log('getting %s for %s %s', this.entityType.name, index, value)

    const criteria: { version?: any; $or?: any } = {}
    criteria[index] = value

    const snapshots = await this.snapshots
      .find(criteria)
      .sort({ version: -1 })
      .limit(-1)
      .toArray()

    const snapshot = snapshots[0]

    if (snapshot) criteria.version = { $gt: snapshot.version }
    const events = await this.events
      .find(criteria)
      .sort({ version: 1 })
      .toArray()
    if (snapshot) delete snapshot._id
    if (!snapshot && !events.length) return

    const id = index === 'id' ? value : snapshot ? snapshot.id : events[0].id

    const entity = this._deserialize(id, snapshot, events)
    return entity
  }

  async getAll(ids?: string[]): Promise<Entity[]> {
    if (!ids) {
      const distinctEventIds = await this.events.distinct('id')
      return await this.getAll(distinctEventIds)
    }

    log('getting %ss for ids %j', this.entityType.name, ids)

    if (ids.length === 0) return []
    const snapshots = await this._getAllSnapshots(ids)
    const entities = await this._getAllEvents(ids, snapshots)
    return entities
  }

  async _commitEvents(entity) {
    if (entity.newEvents.length === 0) return

    if (!entity.id)
      throw new Error(
        `Cannot commit an entity of type ${this.entityType} without an [id] property`
      )

    const events = entity.newEvents
    events.forEach((event) => {
      if (event && event._id) delete event._id // mongo will blow up if we try to insert multiple _id keys
      this.indices.forEach((index) => {
        event[index] = entity[index]
      })
    })
    await this.events.insertMany(events)
    log('committed %s.events for id %s', this.entityType.name, entity.id)
    entity.newEvents = []
  }

  async _commitAllEvents(entities: Entity[]): Promise<void> {
    const events = []
    entities.forEach((entity) => {
      if (entity.newEvents.length === 0) return

      if (!entity.id)
        throw new Error(
          `Cannot commit an entity of type ${this.entityType} without an [id] property`
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

    if (events.length === 0) return

    this.events.insertMany(events)
    log(
      'committed %s.events for ids %j',
      this.entityType.name,
      _.map(entities, 'id')
    )
    entities.forEach((entity) => {
      entity.newEvents = []
    })
  }

  async _commitSnapshots(
    entity: Entity,
    options: { forceSnapshot: boolean }
  ): Promise<any> {
    if (
      options.forceSnapshot ||
      entity.version >= entity.snapshotVersion + this.snapshotFrequency
    ) {
      const snapshot: Snapshot = entity.snapshot() as unknown as Snapshot
      if (snapshot && snapshot._id) delete snapshot._id // mongo will blow up if we try to insert multiple _id keys
      await this.snapshots.insertOne(snapshot)
      log(
        'committed %s.snapshot for id %s %j',
        this.entityType.name,
        entity.id,
        snapshot
      )
      return entity
    } else {
      return entity
    }
  }

  async _commitAllSnapshots(
    entities: Entity[],
    options: { forceSnapshot: boolean }
  ): Promise<SourcedEntity[]> {
    const snapshots = []
    entities.forEach((entity) => {
      if (
        options.forceSnapshot ||
        entity.version >= entity.snapshotVersion + this.snapshotFrequency
      ) {
        const snapshot = entity.snapshot() as unknown as Snapshot
        if (snapshot) {
          if (snapshot._id) delete snapshot._id // mongo will blow up if we try to insert multiple _id keys)
          snapshots.push(snapshot)
        }
      }
    })

    if (snapshots.length === 0) return

    await this.snapshots.insertMany(snapshots)
    log(
      'committed %s.snapshot for ids %s %j',
      this.entityType.name,
      _.map(entities, 'id'),
      snapshots
    )
    return entities
  }

  _deserialize(id, snapshot, events): SourcedEntity {
    log('deserializing %s entity ', this.entityType.name)
    const entity = new this.entityType(snapshot, events)
    entity.id = id
    return entity
  }

  _emitEvents(entity: Entity): void {
    const eventsToEmit = entity.eventsToEmit
    entity.eventsToEmit = []
    eventsToEmit.forEach((eventToEmit) => {
      const args = Array.prototype.slice.call(eventToEmit)
      this.entityType.prototype.emit.apply(entity, args)
    })

    log('emitted local events for id %s', entity.id)
  }

  async _getAllSnapshots(ids: string[]): Promise<Snapshot[]> {
    const match = { $match: { id: { $in: ids } } }
    const sort = { $sort: { snapshotVersion: 1 } }
    const group = {
      $group: { _id: '$id', snapshotVersion: { $last: '$snapshotVersion' } }
    }

    const cursor = this.snapshots.aggregate([match, sort, group])
    const idVersionPairs = await cursor.toArray()
    let criteria: { $or?: any; id?: any; snapshotVersion?: any } = {}
    if (idVersionPairs.length === 0) {
      return []
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
    const snapshots = await this.snapshots.find(criteria).toArray()
    return snapshots
  }

  async _getAllEvents(ids, snapshots): Promise<Entity[]> {
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

    const events = await this.events
      .find(criteria)
      .sort({ id: 1, version: 1 })
      .toArray()
    if (!snapshots.length && !events.length) return
    const results = []
    ids.forEach((id) => {
      const snapshot = _.find(snapshots, (snapshot) => snapshot.id === id)
      if (snapshot) delete snapshot._id
      const evnts = _.filter(events, (event) => event.id === id)
      const entity = this._deserialize(id, snapshot, evnts)
      results.push(entity)
    })
    return results
  }
}
