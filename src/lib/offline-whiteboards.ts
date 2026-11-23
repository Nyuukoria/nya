import type { WhiteboardBoard } from '../types'

const DATABASE = 'nya-offline-notebooks-v1'
const BOARDS = 'boards'
const OUTBOX = 'outbox'
type PendingWrite = { id: string; board?: WhiteboardBoard; create?: boolean; delete?: boolean }
let databasePromise: Promise<IDBDatabase> | undefined

function openDatabase(): Promise<IDBDatabase> {
  if (databasePromise) return databasePromise
  databasePromise = new Promise((resolve, reject) => {
    const request = indexedDB.open(DATABASE, 1)
    request.onupgradeneeded = () => {
      const database = request.result
      if (!database.objectStoreNames.contains(BOARDS)) database.createObjectStore(BOARDS, { keyPath: 'id' })
      if (!database.objectStoreNames.contains(OUTBOX)) database.createObjectStore(OUTBOX, { keyPath: 'id' })
    }
    request.onsuccess = () => {
      request.result.onversionchange = () => { request.result.close(); databasePromise = undefined }
      resolve(request.result)
    }
    request.onerror = () => { databasePromise = undefined; reject(request.error) }
  })
  return databasePromise
}

function transaction<T>(storeName: string, mode: IDBTransactionMode, action: (store: IDBObjectStore) => IDBRequest<T>): Promise<T> {
  return openDatabase().then((database) => new Promise<T>((resolve, reject) => {
    const request = action(database.transaction(storeName, mode).objectStore(storeName))
    request.onsuccess = () => resolve(request.result)
    request.onerror = () => reject(request.error)
  }))
}

// Read the outbox and replace the server snapshot in one transaction, so a
// concurrent checkpoint can never be erased by a refresh.
export async function cacheWhiteboards(boards: WhiteboardBoard[]): Promise<{ boards: WhiteboardBoard[]; queued: boolean }> {
  const database = await openDatabase()
  return new Promise((resolve, reject) => {
    const tx = database.transaction([BOARDS, OUTBOX], 'readwrite')
    const store = tx.objectStore(BOARDS)
    const read = tx.objectStore(OUTBOX).getAll()
    let snapshot = boards
    tx.oncomplete = () => resolve({ boards: snapshot, queued: read.result.length > 0 })
    tx.onabort = () => reject(tx.error)
    read.onsuccess = () => {
      const merged = new Map(boards.map((board) => [board.id, board]))
      for (const pending of read.result as PendingWrite[]) {
        if (pending.delete) merged.delete(pending.id)
        else if (pending.board) merged.set(pending.id, pending.board)
      }
      snapshot = [...merged.values()]
      store.clear()
      snapshot.forEach((board) => store.put(board))
    }
  })
}

export async function cachedWhiteboards(): Promise<WhiteboardBoard[]> {
  return transaction<WhiteboardBoard[]>(BOARDS, 'readonly', (store) => store.getAll())
}

export async function queueWhiteboardWrite(board: WhiteboardBoard, create: boolean): Promise<void> {
  const database = await openDatabase()
  await new Promise<void>((resolve, reject) => {
    const tx = database.transaction([BOARDS, OUTBOX], 'readwrite', { durability: 'strict' })
    const outbox = tx.objectStore(OUTBOX)
    const read = outbox.get(board.id)
    read.onsuccess = () => {
      const pending = read.result as PendingWrite | undefined
      if (pending?.delete || (pending?.board && pending.board.updatedAt > board.updatedAt)) return
      tx.objectStore(BOARDS).put(board)
      outbox.put({ id: board.id, board, create: create || pending?.create, delete: false })
    }
    tx.oncomplete = () => resolve()
    tx.onabort = () => reject(tx.error)
  })
}

export async function acknowledgeWhiteboard(board: WhiteboardBoard, expectedUpdatedAt: string): Promise<void> {
  const database = await openDatabase()
  await new Promise<void>((resolve, reject) => {
    const tx = database.transaction([BOARDS, OUTBOX], 'readwrite')
    const outbox = tx.objectStore(OUTBOX)
    const read = outbox.get(board.id)
    read.onsuccess = () => {
      const pending = read.result as PendingWrite | undefined
      const cached = tx.objectStore(BOARDS)
      if (pending && (pending.delete || pending.board?.updatedAt !== expectedUpdatedAt)) {
        if (pending.create && !pending.delete) outbox.put({ ...pending, create: false })
        return
      }
      const previous = cached.get(board.id)
      previous.onsuccess = () => {
        if (!pending && previous.result?.revision > (board.revision || 0)) return
        cached.put(board)
        outbox.delete(board.id)
      }
    }
    tx.oncomplete = () => resolve()
    tx.onabort = () => reject(tx.error)
  })
}

export async function queueWhiteboardDelete(id: string): Promise<void> {
  const database = await openDatabase()
  await new Promise<void>((resolve, reject) => {
    const tx = database.transaction([BOARDS, OUTBOX], 'readwrite')
    tx.objectStore(BOARDS).delete(id)
    tx.objectStore(OUTBOX).put({ id, delete: true })
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error)
  })
}

export async function pendingWhiteboards(): Promise<Array<{ id: string; board?: WhiteboardBoard; create?: boolean; delete?: boolean }>> {
  return transaction<Array<{ id: string; board?: WhiteboardBoard; create?: boolean; delete?: boolean }>>(OUTBOX, 'readonly', (store) => store.getAll())
}

export async function removePendingWhiteboard(id: string, expectedUpdatedAt?: string): Promise<void> {
  const database = await openDatabase()
  await new Promise<void>((resolve, reject) => {
    const tx = database.transaction(OUTBOX, 'readwrite')
    const store = tx.objectStore(OUTBOX)
    if (!expectedUpdatedAt) store.delete(id)
    else {
      const read = store.get(id)
      read.onsuccess = () => {
        const pending = read.result as { board?: WhiteboardBoard } | undefined
        if (pending?.board?.updatedAt === expectedUpdatedAt) store.delete(id)
      }
    }
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error)
  })
}
