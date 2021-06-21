import { Db, MongoClient } from 'mongodb'
import { Repository } from '../src'
import 'should'

import { SourcedEntity } from 'sourced-ts'

export class Customer extends SourcedEntity {
  id: string
  companyName: string
  constructor(snapshot?, events?) {
    super()
    this.id = null
    this.companyName = null
    this.rehydrate(snapshot, events)
  }

  CreatedEvent(data: { companyName: string }) {
    this.companyName = data.companyName
    this.digest('CreatedEvent', data)
    this.enqueue('customer.created', data)
  }
}

describe('Repository', (): void => {
  let client: MongoClient
  let db: Db
  let repository: Repository

  afterEach(async () => {
    await db.collection('Customer.events').drop()
    await db.collection('Customer.snapshots').drop()
    await client.close()
  })

  beforeEach(async () => {
    client = await MongoClient.connect('mongodb://localhost:27017/test', {
      useNewUrlParser: true,
      useUnifiedTopology: true
    })
    db = client.db()

    repository = new Repository(Customer, { db })
    await repository.init()
  })
  it('should emit to subscribers', async () => {
    const customer = new Customer()
    customer.on('customer.created', (data) => {
      customer.removeAllListeners()
      data.companyName.should.equal('Company, Inc.')
    })
    customer.CreatedEvent({ companyName: 'Company, Inc.' })
    customer.id = '1'
    await repository.commit(customer)
    setTimeout(async () => {
      const cust = await repository.get('1')
      cust.companyName.should.equal('Company, Inc.')
    }, 1000)
  })
})
