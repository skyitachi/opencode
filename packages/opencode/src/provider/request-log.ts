import { Log } from "../util/log"
import { Flag } from "../flag/flag"
import { randomUUID } from "crypto"

export namespace RequestLog {
  export const logger = Log.create({ service: "provider-request" })

  // Configuration for streaming response logging
  const STREAM_LOG_SAMPLE_SIZE = 10000 // Number of lines to sample from streams

  // Generate a unique trace ID for request-response correlation
  export function generateTraceId(): string {
    return randomUUID().split('-')[0] // Use short UUID for readability
  }

  export function logRequest(params: {
    providerID: string
    modelID: string
    url: string
    method: string
    headers: Record<string, any>
    body?: any
    traceId?: string
  }) {
    const { body, ...safeParams } = params
    
    // Log request with body (excluding sensitive data)
    logger.info("API Request", {
      ...safeParams,
      ...(body && { requestBody: typeof body === 'object' ? JSON.stringify(body) : body })
    })
  }

  export function logResponse(params: {
    providerID: string
    modelID: string
    status: number
    headers?: Record<string, any>
    body?: any
    duration: number
    traceId?: string
  }) {
    const { body, ...safeParams } = params
    
    // Handle different response body types
    let loggedBody: string | undefined
    if (body) {
      if (typeof body === 'string') {
        if (body.startsWith('[Streaming Response')) {
          // Already processed streaming response
          loggedBody = body
        } else if (body.startsWith('data: ') || body.includes('\n\ndata: ')) {
          // This looks like SSE (Server-Sent Events) content
          const lines = body.split('\n').filter(line => line.trim())
          loggedBody = `[SSE Stream - ${lines.length} events]`
          
          // Log sample of first few events
          if (lines.length > 0) {
            const sampleLines = lines.slice(0, STREAM_LOG_SAMPLE_SIZE)
            loggedBody += `\nSample: ${sampleLines.join('\n')}${lines.length > STREAM_LOG_SAMPLE_SIZE ? '\n...' : ''}`
          }
        } else {
          loggedBody = body
        }
      } else {
        loggedBody = JSON.stringify(body)
      }
    }
    
    // Log response with body
    logger.info("API Response", {
      ...safeParams,
      ...(loggedBody && { responseBody: loggedBody })
    })
  }

  export function logError(params: {
    providerID: string
    modelID: string
    error: Error
    duration: number
    traceId?: string
  }) {
    logger.error("API Error", {
      providerID: params.providerID,
      modelID: params.modelID,
      error: params.error.message,
      stack: params.error.stack,
      duration: `${params.duration}ms`,
      traceId: params.traceId
    })
  }

  // Advanced function to log streaming responses with optional sampling
  export async function logStreamingResponse(params: {
    providerID: string
    modelID: string
    status: number
    headers: Record<string, string>
    response: Response
    duration: number
    sampleSize?: number
  }) {
    const { response, sampleSize = STREAM_LOG_SAMPLE_SIZE, ...safeParams } = params
    
    try {
      // Clone the response to avoid consuming the original stream
      const responseClone = response.clone()
      const reader = responseClone.body?.getReader()
      
      if (!reader) {
        logResponse({
          ...safeParams,
          headers: Object.fromEntries(response.headers.entries()),
          body: '[Empty Streaming Response]',
          duration: params.duration
        })
        return
      }

      const decoder = new TextDecoder()
      let sampledContent = ''
      let eventCount = 0
      let totalBytes = 0
      let chunksRead = 0

      // Read first few chunks for sampling
      while (chunksRead < sampleSize) {
        const { done, value } = await reader.read()
        if (done) break
        
        const chunk = decoder.decode(value, { stream: true })
        totalBytes += value.length
        chunksRead++
        
        // Count SSE events (lines starting with "data:")
        const dataLines = chunk.split('\n').filter(line => line.trim().startsWith('data:'))
        eventCount += dataLines.length
        
        sampledContent += chunk
      }

      // Cancel the reader - we don't need to read the rest since this is a clone
      reader.cancel()

      // Log the sampled streaming content
      const logBody = `[Streaming Response - ${eventCount} events, ${totalBytes} bytes, ${chunksRead} chunks sampled]`
      const sampleText = sampledContent.trim() ? `\nSample:\n${sampledContent.trim()}` : ''
      
      logResponse({
        ...safeParams,
        headers: Object.fromEntries(response.headers.entries()),
        body: logBody + sampleText,
        duration: params.duration
      })

    } catch (error) {
      logResponse({
        ...safeParams,
        headers: Object.fromEntries(response.headers.entries()),
        body: `[Streaming Response - Unable to sample: ${(error as Error).message}]`,
        duration: params.duration
      })
    }
  }
}
