import 'fake-indexeddb/auto'
import { describe, expect, it } from 'vitest'
import type { WhiteboardBoard } from '../types'
import { acknowledgeWhiteboard, cacheWhiteboards, cachedWhiteboards, pendingWhiteboards, queueWhiteboardDelete, queueWhiteboardWrite } from './offline-whiteboards'

function board(id: string, minute: number, revision = 1): WhiteboardBoard {
  const updatedAt = new Date(Date.UTC(2026, 0, 1, 0, minute)).toISOString()
  return { id, title: `Draft ${minute}`, createdAt: updatedAt, updatedAt, revision, published: false, pages: [] }
}

describe('offline notebook recovery', () => {
  it('keeps a newer checkpoint after an older upload acknowledges or falls back', async () => {
    const old = board('writing', 1)
    const latest = board('writing', 2)
    await queueWhiteboardWrite(latest, false)
    await acknowledgeWhiteboard({ ...old, revision: 2 }, old.updatedAt)
    await queueWhiteboardWrite(old, false)
    expect((await cachedWhiteboards()).find(({ id }) => id === latest.id)).toEqual(latest)
    expect((await pendingWhiteboards()).find(({ id }) => id === latest.id)?.board).toEqual(latest)
    await acknowledgeWhiteboard({ ...latest, revision: 3 }, latest.updatedAt)
    expect((await pendingWhiteboards()).some(({ id }) => id === latest.id)).toBe(false)
    expect((await cachedWhiteboards()).find(({ id }) => id === latest.id)?.revision).toBe(3)
  })

  it('restores pending drafts over a server refresh without restoring deleted boards', async () => {
    const old = board('reload', 1)
    const draft = board('reload', 2)
    const deleted = board('deleted', 1)
    await queueWhiteboardWrite(draft, true)
    await queueWhiteboardWrite(draft, false)
    await queueWhiteboardDelete(deleted.id)
    await acknowledgeWhiteboard(deleted, deleted.updatedAt)
    expect(await cacheWhiteboards([old, deleted])).toEqual({ boards: [draft], queued: true })
    expect(await cachedWhiteboards()).toEqual([draft])
    expect((await pendingWhiteboards()).find(({ id }) => id === draft.id)?.create).toBe(true)
  })
})
