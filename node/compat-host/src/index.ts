import { createCompatHost, createDecoder } from './host.ts'

const host = createCompatHost()
const decoder = createDecoder()
let stopped = false

function stop(error?: unknown) {
  if (stopped) return
  stopped = true
  void host.stop(error)
}

process.stdin.on('data', (chunk: Uint8Array) => {
  if (stopped) return
  try {
    for (const frame of decoder.push(chunk)) void host.receive(frame)
  } catch (error) {
    stop(error)
  }
})
process.stdin.on('end', () => stop())
process.stdin.on('error', stop)
process.on('uncaughtException', stop)
process.on('unhandledRejection', stop)
