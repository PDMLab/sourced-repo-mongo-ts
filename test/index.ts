import { SourcedEntity as Entity } from 'sourced-ts'
import { Repository } from '../src/index'
import { Db, MongoClient } from 'mongodb'

import 'should'
import should from 'should'

let db: Db
let dbClient: MongoClient

/* Market model/entity */
class Market extends Entity {
  orders: any[]
  price: number
  id: any
  constructor(snapshot?, events?) {
    super()
    this.orders = []
    this.price = 0
    this.rehydrate(snapshot, events)
  }

  init(param) {
    this.id = param.id
    this.digest('init', param)
    this.emit('initialized', param, this)
  }

  createOrder(param) {
    this.orders.push(param)
    let total = 0
    this.orders.forEach(function (order) {
      total += order.price
    })
    this.price = total / this.orders.length
    this.digest('createOrder', param)
    this.emit('done', param, this)
  }
}
/* end Market model/entity */

const dropEventsCollection = (db): Promise<void> => {
  return new Promise((resolve, reject) => {
    db.collection('Market.events').drop((err, ok) => {
      if (
        err &&
        err.codeName &&
        err.codeName === 'BackgroundOperationInProgressForNamespace'
      ) {
        setTimeout(async () => {
          return resolve(await dropEventsCollection(db))
        }, 100)
      } else if (err) {
        return resolve()
      } else {
        return resolve()
      }
    })
  })
}
const dropSnapshotsCollection = (db): Promise<void> => {
  return new Promise((resolve, reject) => {
    db.collection('Market.snapshots').drop((err, ok) => {
      if (
        err &&
        err.codeName &&
        err.codeName === 'BackgroundOperationInProgressForNamespace'
      ) {
        setTimeout(async () => {
          return resolve(await dropSnapshotsCollection(db))
        }, 100)
      } else {
        return resolve()
      }
    })
  })
}

describe('Repository', function () {
  let repository

  afterEach(async () => {
    await dropEventsCollection(db)
    await dropSnapshotsCollection(db)
    await dbClient.close()
  })

  beforeEach((done) => {
    MongoClient.connect('mongodb://127.0.0.1:27017/sourced', {
      useNewUrlParser: true,
      useUnifiedTopology: true
    }).then((client) => {
      // console.log("connected");
      // mongo.once("connected", function (database) {
      db = client.db()
      dbClient = client
      db.collection('Market.events').drop(function () {
        // console.log("dropped events");
        db.collection('Market.snapshots').drop(() => {
          // console.log("dropped snapshots");
          repository = new Repository(Market, { db })
          return done()
        })
      })
    })
  })
  // });

  // after(function (done) {
  //   mongo.close(done);
  // });

  it('should initialize market entity and digest 12 events, setting version, snapshotVersion, and price', function (done) {
    const id = 'somecusip'
    const mrkt = new Market()

    mrkt.init({ id: id })

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 91, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 92, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 93, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 94, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 95, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 91, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 92, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 93, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 94, quantity: 1000 })

    mrkt.should.have.property('version', 12)
    mrkt.should.have.property('snapshotVersion', 0)
    mrkt.should.have.property('price', 92.27272727272727)

    repository.commit(mrkt, function (err) {
      if (err) throw err

      repository.get(id, function (err, market) {
        if (err) throw err

        market.should.have.property('version', 12)
        market.should.have.property('snapshotVersion', 12)
        market.should.have.property('price', 92.27272727272727)

        done()
      })
    })
  })

  it('should load deserialized market entity from snapshot, digest two events, and update version, snapshotVersion, and price', function (done) {
    const id = 'somecusip'
    const mrkt = new Market()

    mrkt.init({ id: id })

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 91, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 92, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 93, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 94, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 95, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 91, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 92, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 93, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 94, quantity: 1000 })

    mrkt.should.have.property('version', 12)
    mrkt.should.have.property('snapshotVersion', 0)
    mrkt.should.have.property('price', 92.27272727272727)

    repository.commit(mrkt, function (err) {
      if (err) throw err

      repository.get(id, function (err, mrkt) {
        if (err) throw err

        mrkt.should.have.property('version', 12)
        mrkt.should.have.property('snapshotVersion', 12)
        mrkt.should.have.property('price', 92.27272727272727)

        mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 })
        mrkt.createOrder({ side: 's', price: 91, quantity: 1000 })

        mrkt.should.have.property('version', 14)
        mrkt.should.have.property('snapshotVersion', 12)
        mrkt.should.have.property('price', 92)
        mrkt.newEvents.should.have.property('length', 2)

        repository.commit(mrkt, function (err) {
          if (err) throw err

          repository.get(id, function (err, market) {
            if (err) throw err

            market.should.have.property('version', 14)
            market.should.have.property('snapshotVersion', 12)
            market.should.have.property('price', 92)
            market.newEvents.should.have.property('length', 0)

            done()
          })
        })
      })
    })
  })

  it('should emit all enqueued eventsToEmit after only after committing', function (done) {
    const id = 'somecusip'
    const mrkt = new Market()

    mrkt.init({ id: id })

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 91, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 92, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 93, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 94, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 95, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 91, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 92, quantity: 1000 })
    mrkt.createOrder({ side: 's', price: 93, quantity: 1000 })
    mrkt.createOrder({ side: 'b', price: 94, quantity: 1000 })

    mrkt.should.have.property('version', 12)
    mrkt.should.have.property('snapshotVersion', 0)
    mrkt.should.have.property('price', 92.27272727272727)

    repository.commit(mrkt, function (err) {
      if (err) throw err

      repository.get(id, function (err, market) {
        if (err) throw err

        market.on('myEventHappened', function (data, data2) {
          market.eventsToEmit.should.have.property('length', 0)
          market.newEvents.should.have.property('length', 0)
          data.should.have.property('data', 'data')
          data2.should.have.property('data2', 'data2')
        })

        market.enqueue('myEventHappened', { data: 'data' }, { data2: 'data2' })

        repository.commit(market, function (err) {
          if (err) throw err

          repository.get(id, function (err, market) {
            if (err) throw err
            market.should.have.property('version', 12)
            market.should.have.property('snapshotVersion', 12)
            market.should.have.property('price', 92.27272727272727)
            market.newEvents.should.have.property('length', 0)
            done()
          })
        })
      })
    })
  })

  it('should load multiple deserialized market entities from snapshot, and commit in bulk', function (done) {
    const id = 'somecusip2'
    const mrkt = new Market()

    const id2 = 'somecusip3'
    const mrkt2 = new Market()

    const id3 = 'somecusip4'
    const mrkt3 = new Market()

    const id4 = 'somecusip5'
    const mrkt4 = new Market()

    mrkt.init({ id: id })
    mrkt2.init({ id: id2 })
    mrkt3.init({ id: id3 })
    mrkt4.init({ id: id4 })

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1001 })

    mrkt2.createOrder({ side: 'b', price: 90, quantity: 1002 })
    mrkt2.createOrder({ side: 'b', price: 90, quantity: 1003 })

    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1004 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1005 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1006 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1007 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1008 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1009 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1010 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1011 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1012 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1013 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1014 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1015 })

    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1016 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1017 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1018 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1019 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1020 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1022 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1023 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1024 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1025 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1026 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1027 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1028 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1029 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1030 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1031 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1032 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1033 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1034 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1035 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1036 })

    repository.commitAll([mrkt, mrkt2, mrkt3, mrkt4], function (err) {
      if (err) return done(err)

      repository.getAll([id, id2, id3, id4], function (err, markets) {
        if (err) return done(err)

        const market = markets[0]
        const market2 = markets[1]
        const market3 = markets[2]
        const market4 = markets[3]

        market.should.have.property('id', id)
        market.should.have.property('version', 2)
        market.should.have.property('snapshotVersion', 0)

        market2.should.have.property('id', id2)
        market2.should.have.property('version', 3)
        market2.should.have.property('snapshotVersion', 0)

        market3.should.have.property('id', id3)
        market3.should.have.property('version', 13)
        market3.should.have.property('snapshotVersion', 13)

        market4.should.have.property('id', id4)
        market4.should.have.property('version', 21)
        market4.should.have.property('snapshotVersion', 21)

        done()
      })
    })
  })

  it('should load all entities when getAll called with callback only', function (done) {
    const id = 'somecusip6'
    const mrkt = new Market()

    const id2 = 'somecusip7'
    const mrkt2 = new Market()

    const id3 = 'somecusip8'
    const mrkt3 = new Market()

    const id4 = 'somecusip9'
    const mrkt4 = new Market()

    mrkt.init({ id: id })
    mrkt2.init({ id: id2 })
    mrkt3.init({ id: id3 })
    mrkt4.init({ id: id4 })

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1001 })

    mrkt2.createOrder({ side: 'b', price: 90, quantity: 1002 })
    mrkt2.createOrder({ side: 'b', price: 90, quantity: 1003 })

    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1004 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1005 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1006 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1007 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1008 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1009 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1010 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1011 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1012 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1013 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1014 })
    mrkt3.createOrder({ side: 'b', price: 90, quantity: 1015 })

    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1016 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1017 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1018 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1019 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1020 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1022 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1023 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1024 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1025 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1026 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1027 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1028 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1029 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1030 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1031 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1032 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1033 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1034 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1035 })
    mrkt4.createOrder({ side: 'b', price: 90, quantity: 1036 })

    repository.commitAll([mrkt, mrkt2, mrkt3, mrkt4], function (err) {
      if (err) return done(err)

      repository.getAll(function (err, markets) {
        if (err) return done(err)

        const market = markets[0]
        const market2 = markets[1]
        const market3 = markets[2]
        const market4 = markets[3]

        market.should.have.property('id', id)
        market.should.have.property('version', 2)
        market.should.have.property('snapshotVersion', 0)

        market2.should.have.property('id', id2)
        market2.should.have.property('version', 3)
        market2.should.have.property('snapshotVersion', 0)

        market3.should.have.property('id', id3)
        market3.should.have.property('version', 13)
        market3.should.have.property('snapshotVersion', 13)

        market4.should.have.property('id', id4)
        market4.should.have.property('version', 21)
        market4.should.have.property('snapshotVersion', 21)

        done()
      })
    })
  })

  it('should take snapshot when forceSnapshot provided', function (done) {
    const id = 'somecusip6'

    const mrkt = new Market()

    mrkt.init({ id: id })

    mrkt.createOrder({ side: 'b', price: 90, quantity: 1000 })

    mrkt.should.have.property('version', 2)
    mrkt.should.have.property('snapshotVersion', 0)
    mrkt.should.have.property('price', 90)

    repository.commit(mrkt, { forceSnapshot: true }, function (err) {
      if (err) throw err

      repository.get(id, function (err, market) {
        if (err) throw err

        market.should.have.property('version', 2)
        market.should.have.property('snapshotVersion', 2)
        market.should.have.property('price', 90)

        done()
      })
    })
  })

  it('should return null when get called with id of nonexisting entity', function (done) {
    repository.get('fake', function (err, market) {
      if (err) throw err

      should.not.exist(market)

      done()
    })
  })

  it('should return null when getAll called with only ids of nonexisting entities', function (done) {
    repository.getAll(['fake'], function (err, market) {
      if (err) throw err

      should.not.exist(market)

      done()
    })
  })
})
